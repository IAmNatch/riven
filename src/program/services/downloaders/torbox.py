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
from program.utils.request import CircuitBreakerOpen, SmartResponse, SmartSession
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

        self.session = SmartSession(
            base_url=self.BASE_URL,
            rate_limits={
                # TorBox documents ~60 req/min on most endpoints.
                "api.torbox.app": {
                    "rate": 60 / 60,
                    "capacity": 60,
                },
            },
            proxies=proxies,
            retries=2,
            backoff_factor=0.5,
        )

        try:
            version = get_version()
        except Exception:
            version = "Unknown"

        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                # Riven user agent for TorBox analytics
                "User-Agent": f"Riven/{version} TorBox/1.0",
            }
        )


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
        Attempt a quick availability check by adding the magnet to TorBox and checking
        whether it is instantly available (already cached). The added torrent id and info
        are cached on the returned container to avoid re-adding/re-fetching in the download phase.
        """

        torrent_id: int | None = None

        try:
            torrent_id = self.add_torrent(infohash)
            container, reason, info = self._process_torrent(
                torrent_id, infohash, item_type
            )

            if container is None and reason:
                logger.debug(f"Availability check failed [{infohash}]: {reason}")

                # Failed validation - delete the torrent
                if torrent_id:
                    try:
                        self.delete_torrent(torrent_id)
                    except Exception as e:
                        logger.debug(
                            f"Failed to delete failed torrent {torrent_id}: {e}"
                        )

                return None

            # Success - cache torrent_id AND info in container to avoid re-adding/re-fetching during download
            if container:
                container.torrent_id = torrent_id
                container.torrent_info = info

            return container

        except CircuitBreakerOpen:
            # Don't swallow the breaker; upstream orchestration decides backoff policy.
            logger.debug(f"Circuit breaker OPEN for TorBox; skipping {infohash}")

            if torrent_id:
                try:
                    self.delete_torrent(torrent_id)
                except Exception:
                    pass

            raise
        except TorBoxError as e:
            logger.warning(f"Availability check failed [{infohash}]: {e}")

            if torrent_id:
                try:
                    self.delete_torrent(torrent_id)
                except Exception:
                    pass

            return None
        except InvalidDebridFileException as e:
            logger.debug(
                f"Availability check failed [{infohash}]: Invalid debrid file(s) - {e}"
            )

            if torrent_id:
                try:
                    self.delete_torrent(torrent_id)
                except Exception:
                    pass

            return None
        except Exception as e:
            logger.debug(f"Availability check failed [{infohash}]: {e}")

            if torrent_id:
                try:
                    self.delete_torrent(torrent_id)
                except Exception:
                    pass

            return None

    def _process_torrent(
        self,
        torrent_id: int,
        infohash: str,
        item_type: ProcessedItemType,
    ) -> tuple[TorrentContainer | None, str | None, TorrentInfo | None]:
        """
        Process a single torrent and return (container, reason, info).

        Returns:
            (TorrentContainer or None, human-readable reason string if None, TorrentInfo or None)
        """

        info = self.get_torrent_info(torrent_id)

        if not info:
            return None, "no torrent info returned by TorBox", None

        if not info.files:
            return None, "no files present in the torrent", None

        # get_torrent_info() normalizes status to "cached" when the torrent is ready.
        if info.status not in READY_STATES:
            return None, f"Not instantly available (status={info.status})", None

        files = list[DebridFile]()

        for file_id, meta in info.files.items():
            try:
                df = DebridFile.create(
                    path=meta.path,
                    filename=meta.filename,
                    filesize_bytes=meta.bytes,
                    filetype=item_type,
                    file_id=file_id,
                )

                # download_url holds the re-resolvable handle built in get_torrent_info().
                if meta.download_url:
                    df.download_url = meta.download_url

                files.append(df)
            except InvalidDebridFileException as e:
                logger.debug(f"{infohash}: {e}")

        if not files:
            return None, "no valid files after validation", None

        # Return container WITH the TorrentInfo to avoid re-fetching in download phase
        return TorrentContainer(infohash=infohash, files=files), None, info

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

            response = self.api.session.get(
                "torrents/requestdl",
                params={
                    "token": self.api.api_key,
                    "torrent_id": torrent_id_str,
                    "file_id": file_id_str,
                },
            )

            self._maybe_backoff(response)

            if response.status_code == 404:
                logger.warning(f"TorBox link unavailable (404) for handle {link}")
                raise DebridServiceLinkUnavailable(provider=self.key, link=link)

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
