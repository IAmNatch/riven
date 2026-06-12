from __future__ import annotations

from datetime import datetime
from typing import Any

from loguru import logger

from program.services.downloaders.models import (
    DebridFile,
    InvalidDebridFileException,
    TorrentContainer,
    TorrentFile,
    TorrentInfo,
    UserInfo,
    UnrestrictedLink,
)
from program.settings import settings_manager
from program.utils import get_version
from program.utils.request import CircuitBreakerOpen, SmartResponse, SmartSession, TokenBucket
from program.services.streaming.exceptions.debrid_service_exception import (
    DebridServiceLinkUnavailable,
)
from program.media.item import ProcessedItemType

from .shared import DownloaderBase, premium_days_left


# TorBox download_state values that indicate the torrent is ready for streaming.
# Readiness is primarily driven by the `download_present` flag; these are a fallback.
READY_STATES = {"cached", "completed", "uploading"}

# TorBox has no per-file restricted link in its listing; a CDN URL is fetched via the
# requestdl endpoint using (torrent_id, file_id). We store the requestdl endpoint URL
# itself (with redirect=true) as DebridFile.download_url. That URL is:
#   1. A real https URL, so it never breaks httpx and is directly streamable (it 307s
#      to the CDN), which is the fallback the VFS uses when unrestrict_link fails.
#   2. Stable and re-resolvable: unrestrict_link() parses (torrent_id, file_id) back out
#      and requests a fresh direct CDN URL, mirroring the restricted -> unrestricted
#      pattern used by the other downloaders.
# HANDLE_PREFIX is the legacy torbox://{torrent_id}/{file_id} form, still parsed for
# backward compatibility with entries created before the requestdl-URL handle.
HANDLE_PREFIX = "torbox://"

# Per-endpoint rate limits. TorBox allows 300/min per token across all endpoints, EXCEPT the
# create endpoints (createtorrent/usenet/webdl) at 60/HOUR per token.
#
# SmartSession limiters AND circuit breakers are per-DOMAIN, and SmartSession auto-trips the
# breaker on every 429/5xx. So we use TWO sessions that split the one 300/min token budget:
#   - control session: checkcached / mylist / createtorrent / controltorrent / user
#   - stream session : requestdl only (playback link resolution)
# This gives requestdl its OWN breaker, so a dead/evicted torrent's requestdl 5xx can never trip
# the breaker that gates downloads (createtorrent) and availability (checkcached). createtorrent
# additionally gets a dedicated 60/hour bucket so it self-paces instead of 429ing.
CONTROL_RATE_PER_MIN = 200       # checkcached/mylist/createtorrent/controltorrent/user
STREAM_RATE_PER_MIN = 90         # requestdl only (low-volume; isolated breaker) -> 290 total
DOMAIN_BURST = 5                 # small burst allowance per session
CREATETORRENT_PER_HOUR = 58      # createtorrent only; under the 60/hour cap for safety margin


class TorBoxError(Exception):
    """Base exception for TorBox related errors."""


def _unwrap(response: SmartResponse) -> Any:
    """Extract the `data` field from a TorBox API envelope.

    TorBox wraps every response as {"success": bool, "error": ..., "detail": str, "data": ...}.
    """

    payload = response.json()

    if isinstance(payload, dict):
        return payload.get("data")

    return payload


class TorBoxAPI:
    """
    Minimal TorBox API client using SmartSession for retries, rate limits, and circuit breaker.
    """

    BASE_URL = "https://api.torbox.app/v1/api"

    def __init__(self, api_key: str, proxy_url: str | None = None) -> None:
        """
        Args:
            api_key: TorBox API key.
            proxy_url: Optional proxy URL used for both HTTP and HTTPS.
        """

        self.api_key = api_key
        self.proxy_url = proxy_url

        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        # Control session: account/download operations (checkcached, mylist, createtorrent,
        # controltorrent, user). Its breaker is the one that must stay healthy for the pipeline.
        self.session = SmartSession(
            base_url=self.BASE_URL,
            rate_limits={
                "api.torbox.app": {
                    "rate": CONTROL_RATE_PER_MIN / 60,
                    "capacity": DOMAIN_BURST,
                },
            },
            proxies=proxies,
            retries=2,
            backoff_factor=0.5,
        )

        # Stream session: requestdl only. Isolated breaker so a dead/evicted torrent's 5xx during
        # playback-link resolution can never trip the control breaker (downloads/availability).
        self.stream_session = SmartSession(
            base_url=self.BASE_URL,
            rate_limits={
                "api.torbox.app": {
                    "rate": STREAM_RATE_PER_MIN / 60,
                    "capacity": DOMAIN_BURST,
                },
            },
            proxies=proxies,
            retries=2,
            backoff_factor=0.5,
        )

        # Dedicated bucket for createtorrent (60/hour cap). capacity=1 => no burst, paces at
        # ~CREATETORRENT_PER_HOUR/hour. Gated in add_torrent() before each createtorrent call,
        # so cached-download adds self-throttle instead of 429ing and tripping the breaker.
        self.createtorrent_limiter = TokenBucket(
            rate=CREATETORRENT_PER_HOUR / 3600,
            capacity=1,
            name="api.torbox.app/createtorrent",
        )

        try:
            version = get_version()
        except Exception:
            version = "Unknown"

        headers = {
            "Authorization": f"Bearer {api_key}",
            # Riven user agent for TorBox analytics
            "User-Agent": f"Riven/{version} TorBox/1.0",
        }
        self.session.headers.update(headers)
        self.stream_session.headers.update(headers)


class TorBoxDownloader(DownloaderBase):
    """
    TorBox downloader with lean exception handling.

    Notes on failure & breaker behaviour:
    - Network/transport failures are retried by SmartSession, then counted against the per-domain
      CircuitBreaker; once OPEN, SmartSession raises CircuitBreakerOpen before the request.
    - HTTP status codes are not exceptions; we check response.ok and map to messages via _handle_error(...).
    - TorBox returns ready files directly from its listing, so there is no waiting_files_selection step.
    """

    def __init__(self) -> None:
        self.key = "torbox"
        self.settings = settings_manager.settings.downloaders.torbox
        self.api: TorBoxAPI | None = None
        self.initialized = self.validate()

    def validate(self) -> bool:
        """
        Validate settings and current premium status.

        Returns:
            True if ready, else False.
        """

        if not self._validate_settings():
            return False

        proxy_url = self.PROXY_URL or None
        self.api = TorBoxAPI(api_key=self.settings.api_key, proxy_url=proxy_url)

        return self._validate_premium()

    def _validate_settings(self) -> bool:
        """
        Returns:
            True when enabled and API key present; otherwise False.
        """

        if not self.settings.enabled:
            return False

        if not self.settings.api_key:
            logger.warning("TorBox API key is not set")
            return False

        return True

    def _validate_premium(self) -> bool:
        """
        Returns:
            True if premium membership is active; otherwise False.
        """

        try:
            user_info = self.get_user_info()

            if not user_info:
                logger.error("Failed to retrieve TorBox user info")
                return False

            if user_info.premium_status != "premium":
                logger.error("TorBox premium membership required")
                return False

            if user_info.premium_expires_at:
                logger.info(premium_days_left(user_info.premium_expires_at))

            return True
        except Exception as e:
            logger.error(f"Failed to validate TorBox premium status: {e}")
            return False

    def _maybe_backoff(self, response: SmartResponse) -> None:
        """
        Promote TorBox 429/5xx responses to a service-level backoff signal.
        """

        code = response.status_code

        if code == 429 or (500 <= code < 600):
            # Name matches the breaker key in SmartSession rate_limits/breakers
            raise CircuitBreakerOpen("api.torbox.app")

    def _handle_error(self, response: SmartResponse) -> str:
        """
        Map HTTP status codes to normalized error messages for logs/exceptions.
        """

        code = response.status_code

        mapping = {
            400: "[400] Bad request",
            401: "[401] Unauthorized - check API key",
            403: "[403] Forbidden",
            404: "[404] Torrent Not Found or Service Unavailable",
            429: "[429] Rate Limit Exceeded",
            503: "[503] Service Unavailable",
        }

        if code in mapping:
            return mapping[code]

        try:
            payload = response.json()

            if isinstance(payload, dict) and payload.get("detail"):
                return str(payload["detail"])
        except Exception:
            pass

        return response.reason or f"HTTP {code}"

    def get_instant_availability(
        self,
        infohash: str,
        item_type: ProcessedItemType,
        **kwargs: Any,
    ) -> TorrentContainer | None:
        """
        Check cached availability via the `checkcached` endpoint WITHOUT creating a torrent.

        TorBox limits `createtorrent` to 60/hour and its abuse system flags rapid
        add/delete churn, so cache-probing must not add (and then delete) a torrent for
        every candidate stream. `checkcached` (300/min, no side effects) tells us whether
        the infohash is cached and lists its files using the SAME file ids the torrent will
        have once added. The returned container therefore carries NO torrent_id / torrent_info
        and its files have NO download_url yet: the torrent is created lazily at download time
        (see Downloader.download_cached_stream_on_service), which is the only place we ever
        call `createtorrent`.
        """

        try:
            entry = self._check_cached(infohash)

            if not entry:
                return None

            container, reason = self._build_container(entry, infohash, item_type)

            if container is None:
                if reason:
                    logger.debug(f"Availability check failed [{infohash}]: {reason}")
                return None

            return container

        except CircuitBreakerOpen:
            # Don't swallow the breaker; upstream orchestration decides backoff policy.
            logger.debug(f"Circuit breaker OPEN for TorBox; skipping {infohash}")
            raise
        except TorBoxError as e:
            logger.warning(f"Availability check failed [{infohash}]: {e}")
            return None
        except Exception as e:
            logger.debug(f"Availability check failed [{infohash}]: {e}")
            return None

    def _check_cached(self, infohash: str) -> dict[str, Any] | None:
        """
        Return the `checkcached` entry (name/size/hash/files) for an infohash, or None if
        it is not cached on TorBox. Uses the no-side-effect `checkcached` endpoint.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            TorBoxError: If the API returns a failing status.
        """

        assert self.api

        response = self.api.session.get(
            "torrents/checkcached",
            params={"hash": infohash, "format": "object", "list_files": "true"},
        )

        self._maybe_backoff(response)

        if not response.ok:
            raise TorBoxError(self._handle_error(response))

        data = _unwrap(response)

        if not data:
            return None

        # format=object -> {hash: {name, size, hash, files: [...]}}. Be lenient about key case.
        if isinstance(data, dict):
            entry = (
                data.get(infohash)
                or data.get(infohash.lower())
                or data.get(infohash.upper())
            )
            if entry is None and len(data) == 1:
                entry = next(iter(data.values()))
            return entry if isinstance(entry, dict) else None

        if isinstance(data, list):
            return data[0] if data and isinstance(data[0], dict) else None

        return None

    def _build_container(
        self,
        entry: dict[str, Any],
        infohash: str,
        item_type: ProcessedItemType,
    ) -> tuple[TorrentContainer | None, str | None]:
        """
        Build a TorrentContainer from a `checkcached` entry. Files keep the TorBox file id
        (same id `requestdl`/`mylist` use), but no download_url is set yet — those are
        materialized at download time once the torrent exists.

        Returns:
            (TorrentContainer or None, human-readable reason string if None)
        """

        files_raw = entry.get("files") or []

        if not files_raw:
            return None, "no files present in the torrent"

        files = list[DebridFile]()

        for f in files_raw:
            file_id = f.get("id")

            # file_id can legitimately be 0, so compare against None explicitly.
            if file_id is None:
                continue

            filename = f.get("short_name") or (f.get("name") or "").split("/")[-1]
            path = f.get("name") or filename

            try:
                df = DebridFile.create(
                    path=path,
                    filename=filename,
                    filesize_bytes=f.get("size") or 0,
                    filetype=item_type,
                    file_id=file_id,
                )
                # download_url intentionally left None; set in the download phase.
                files.append(df)
            except InvalidDebridFileException as e:
                logger.debug(f"{infohash}: {e}")

        if not files:
            return None, "no valid files after validation"

        return TorrentContainer(infohash=infohash, files=files), None

    def add_torrent(self, infohash: str) -> int:
        """
        Add a magnet by infohash.

        Returns:
            TorBox torrent id.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            TorBoxError: If the API returns a failing status.
        """

        assert self.api

        # Proactively pace createtorrent to its 60/hour cap (blocks until a token is free)
        # so we never 429 on it or trip the shared per-domain breaker.
        self.api.createtorrent_limiter.wait()

        magnet = f"magnet:?xt=urn:btih:{infohash}"

        response = self.api.session.post(
            "torrents/createtorrent",
            data={"magnet": magnet.lower()},
        )

        self._maybe_backoff(response)

        if not response.ok:
            raise TorBoxError(self._handle_error(response))

        data = _unwrap(response)

        if not data or data.get("torrent_id") is None:
            raise TorBoxError("No torrent ID returned by TorBox")

        return int(data["torrent_id"])

    def select_files(self, torrent_id: int | str, file_ids: list[int] | None = None) -> None:
        """
        Select which files to download from the torrent.

        Note: TorBox does not require explicit file selection; cached files are
        immediately available once the torrent is ready.
        """

        pass

    def get_torrent_info(self, torrent_id: int | str) -> TorrentInfo:
        """
        Retrieve torrent information and normalize into TorrentInfo.

        Each file's download_url is set to a re-resolvable handle (HANDLE_PREFIX + torrent_id/file_id)
        that unrestrict_link() resolves to a temporary CDN URL via the requestdl endpoint.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            TorBoxError: If the API returns a failing status.
        """

        assert self.api

        response = self.api.session.get(
            "torrents/mylist",
            params={"id": str(torrent_id), "bypass_cache": "true"},
        )

        self._maybe_backoff(response)

        if not response.ok:
            logger.debug(
                f"Failed to get torrent info for {torrent_id}: {self._handle_error(response)}"
            )
            raise TorBoxError(self._handle_error(response))

        data = _unwrap(response)

        # mylist?id= returns a single object; defensively handle a list form too.
        if isinstance(data, list):
            data = data[0] if data else None

        if not data:
            raise TorBoxError(f"Torrent {torrent_id} not found")

        download_present = bool(data.get("download_present"))
        download_state = data.get("download_state") or "unknown"
        progress_raw = data.get("progress") or 0

        ready = (
            download_present
            or download_state in READY_STATES
            or progress_raw >= 1
        )
        status = "cached" if ready else download_state

        files = dict[int, TorrentFile]()

        for f in data.get("files", []) or []:
            file_id = f.get("id")

            # file_id can legitimately be 0, so compare against None explicitly.
            if file_id is None:
                continue

            files[file_id] = TorrentFile(
                id=file_id,
                path=f.get("name") or f.get("short_name") or "",
                bytes=f.get("size") or 0,
                selected=1,
                download_url=self._build_download_url(torrent_id, file_id),
            )

        created_at = None
        raw_created = data.get("created_at")

        if raw_created:
            try:
                created_at = datetime.fromisoformat(raw_created.replace("Z", "+00:00"))
            except Exception:
                pass

        # TorBox reports progress as a 0..1 fraction; normalize to 0..100 like the other services.
        try:
            progress = float(progress_raw) * 100 if progress_raw <= 1 else float(progress_raw)
        except Exception:
            progress = 0.0

        return TorrentInfo(
            id=data.get("id", torrent_id),
            name=data.get("name") or "",
            status=status,
            infohash=data.get("hash"),
            bytes=data.get("size"),
            created_at=created_at,
            progress=progress,
            files=files,
            links=[],
        )

    def delete_torrent(self, torrent_id: int | str) -> None:
        """
        Delete a torrent on TorBox.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            TorBoxError: If the API returns a failing status.
        """

        assert self.api

        response = self.api.session.post(
            "torrents/controltorrent",
            json={"torrent_id": int(torrent_id), "operation": "delete"},
        )

        self._maybe_backoff(response)

        if not response.ok:
            raise TorBoxError(self._handle_error(response))

    def _build_download_url(self, torrent_id: int | str, file_id: int) -> str:
        """
        Build the stable, directly-streamable download handle for a file.

        This is the requestdl endpoint URL with redirect=true: a real https URL that
        307-redirects to a fresh CDN URL, so it works both as a direct stream fallback
        and as a re-resolvable handle for unrestrict_link().
        """

        assert self.api

        return (
            f"{self.api.BASE_URL}/torrents/requestdl"
            f"?token={self.api.api_key}"
            f"&torrent_id={torrent_id}&file_id={file_id}&redirect=true"
        )

    def _parse_handle(self, link: str) -> tuple[str | None, str | None]:
        """
        Extract (torrent_id, file_id) from a download handle.

        Supports both the current requestdl-URL form and the legacy torbox:// form.
        """

        # Legacy form: torbox://{torrent_id}/{file_id}
        if link.startswith(HANDLE_PREFIX):
            ref = link[len(HANDLE_PREFIX):]
            torrent_id_str, _, file_id_str = ref.partition("/")

            if torrent_id_str and file_id_str != "":
                return torrent_id_str, file_id_str

            return None, None

        # Current form: requestdl endpoint URL with query params
        if "torrents/requestdl" in link:
            from urllib.parse import urlparse, parse_qs

            query = parse_qs(urlparse(link).query)
            torrent_id_str = (query.get("torrent_id") or [None])[0]
            file_id_str = (query.get("file_id") or [None])[0]

            if torrent_id_str is not None and file_id_str is not None:
                return torrent_id_str, file_id_str

        return None, None

    def unrestrict_link(self, link: str) -> UnrestrictedLink | None:
        """
        Resolve a stored TorBox handle into a fresh direct CDN download URL via the
        requestdl endpoint (called without redirect so the URL is returned in the body).

        Returns:
            UnrestrictedLink with the direct download URL, or None on error.

        Raises:
            DebridServiceLinkUnavailable: When the underlying torrent/file is gone (404),
                so the VFS layer can trigger a fresh download.
        """

        try:
            assert self.api

            torrent_id_str, file_id_str = self._parse_handle(link)

            if torrent_id_str is None or file_id_str is None:
                logger.debug(f"TorBox unrestrict: cannot parse handle: {link}")
                return None

            # requestdl goes through the isolated stream session, so its failures trip the
            # stream breaker only -- never the control breaker that gates downloads/availability.
            response = self.api.stream_session.get(
                "torrents/requestdl",
                params={
                    "token": self.api.api_key,
                    "torrent_id": torrent_id_str,
                    "file_id": file_id_str,
                },
            )

            # 404, or a 500 DATABASE_ERROR, means the torrent is gone from the account (TorBox
            # returns either for an evicted torrent -- and crucially mylist 500s for it too, so
            # there is no reliable presence check). Signal the VFS to re-download so the entry
            # self-heals. We deliberately do NOT probe the control session here: routing an
            # evicted-torrent check through it would trip the control breaker, defeating the whole
            # point of the separate stream session. The stream breaker absorbs these 5xx in
            # isolation, so downloads/availability keep working.
            if response.status_code in (404, 500):
                logger.warning(
                    f"TorBox link unavailable ({response.status_code}) for handle {link}; "
                    f"signalling re-download"
                )
                raise DebridServiceLinkUnavailable(provider=self.key, link=link)

            # Other transient 429/5xx (e.g. 502/503) -> stream-breaker backoff signal, retried later.
            self._maybe_backoff(response)

            if not response.ok:
                logger.warning(
                    f"TorBox requestdl failed [{response.status_code}]: {self._handle_error(response)}"
                )
                return None

            data = _unwrap(response)

            if not data or not isinstance(data, str):
                logger.warning("TorBox requestdl returned no download URL")
                return None

            return UnrestrictedLink(download=data, filename="", filesize=0)
        except DebridServiceLinkUnavailable:
            raise
        except CircuitBreakerOpen:
            raise
        except Exception as e:
            logger.debug(f"TorBox unrestrict_link failed for {link}: {e}")
            return None

    def get_user_info(self) -> UserInfo | None:
        """
        Get normalized user information from TorBox.

        Returns:
            UserInfo: Normalized user information including premium status and expiration.
        """

        try:
            assert self.api

            response = self.api.session.get("user/me")
            self._maybe_backoff(response)

            if not response.ok:
                logger.error(f"Failed to get user info: {self._handle_error(response)}")
                return None

            data = _unwrap(response)

            if not data:
                return None

            plan = data.get("plan", 0) or 0

            expiration = None
            premium_days = None
            raw_exp = data.get("premium_expires_at")

            if raw_exp:
                try:
                    expiration = datetime.fromisoformat(raw_exp.replace("Z", "+00:00"))
                    premium_days = (expiration - datetime.now(expiration.tzinfo)).days
                except Exception as e:
                    logger.debug(f"Failed to parse TorBox expiration date: {e}")

            return UserInfo(
                service="torbox",
                username=data.get("email"),
                email=data.get("email"),
                user_id=data.get("id"),
                premium_status="premium" if plan > 0 else "free",
                premium_expires_at=(
                    expiration.replace(tzinfo=None) if expiration else None
                ),
                premium_days_left=premium_days,
                total_downloaded_bytes=data.get("total_bytes_downloaded"),
            )
        except CircuitBreakerOpen as e:
            logger.warning(f"Circuit breaker OPEN while getting TorBox user info: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to get TorBox user info: {e}")
            return None
