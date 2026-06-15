import time
from typing import Literal, cast
from pydantic import BaseModel, field_validator
import regex
from loguru import logger
from plexapi.library import LibrarySection
from plexapi.myplex import MyPlexAccount
from plexapi.server import PlexServer
from plexapi.video import Movie, Show
from plexapi.media import Guid

from program.settings import settings_manager
from program.utils.request import SmartSession

TMDBID_REGEX = regex.compile(r"tmdb://(\d+)")
TVDBID_REGEX = regex.compile(r"tvdb://(\d+)")

# Backpressure for path-scoped section refreshes. section.update() is
# fire-and-forget: it returns immediately while Plex scans asynchronously. When
# many items complete at once (e.g. a multi-season show, or a download backlog
# draining), the Updater would fire a flood of refreshes that Plex runs
# overlapping -- opening many files on the debrid VFS simultaneously and getting
# the debrid provider rate-limited (429s). To prevent that we wait for each scan
# to finish before returning. Combined with the single-threaded Updater executor
# (max_workers=1), this serializes the whole burst into one scan at a time.
SECTION_REFRESH_POLL_INTERVAL = 2.0   # seconds between "is it still scanning?" checks
SECTION_REFRESH_MAX_WAIT = 180.0      # cap so a foreign/stuck scan can't block forever


class GuidModel(BaseModel):
    id: str

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not any(
            [
                v.startswith("imdb://"),
                v.startswith("tmdb://"),
                v.startswith("tvdb://"),
            ]
        ):
            raise ValueError(f"Invalid GUID format: {v}")

        return v


class PlexAPIError(Exception):
    """Base exception for PlexApi related errors"""


class PlexAPI:
    """Handles Plex API communication"""

    def __init__(self, token: str, base_url: str):
        self.rss_urls: list[str] | None = None
        self.token = token
        self.BASE_URL = base_url

        self.session = SmartSession(
            rate_limits={
                # 1 call per second, 60 calls per minute
                "metadata.provider.plex.tv": {
                    "rate": 1,
                    "capacity": 60,
                },
            },
            retries=3,
            backoff_factor=0.3,
        )

        self.account = None
        self.plex_server = None
        self.rss_enabled = False

    def validate_account(self):
        try:
            self.account = MyPlexAccount(session=self.session, token=self.token)
        except Exception as e:
            logger.error(f"Failed to authenticate Plex account: {e}")
            return False

        return True

    def validate_server(self):
        self.plex_server = PlexServer(
            self.BASE_URL, token=self.token, session=self.session, timeout=60
        )

    def set_rss_urls(self, rss_urls: list[str]):
        self.rss_urls = rss_urls

    def clear_rss_urls(self):
        self.rss_urls = None
        self.rss_enabled = False

    def validate_rss(self, url: str):
        return self.session.get(url)

    def ratingkey_to_imdbid(self, ratingKey: str) -> str | None:
        """Convert Plex rating key to IMDb ID"""

        token = settings_manager.settings.updaters.plex.token
        filter_params = (
            "includeGuids=1&includeFields=guid,title,year&includeElements=Guid"
        )
        url = f"https://metadata.provider.plex.tv/library/metadata/{ratingKey}?X-Plex-Token={token}&{filter_params}"

        response = self.session.get(url)

        class ResponseData(BaseModel):
            class MediaContainerModel(BaseModel):

                class MetadataModel(BaseModel):

                    Guid: list[GuidModel]

                Metadata: list[MetadataModel]

            MediaContainer: MediaContainerModel

        if response.ok:
            data = ResponseData.model_validate(response.json())

            if not data.MediaContainer.Metadata:
                logger.debug(f"No metadata found for rating key: {ratingKey}")
                return None

            return next(
                (
                    guid.id.split("//")[-1]
                    for guid in data.MediaContainer.Metadata[0].Guid
                    if "imdb://" in guid.id
                ),
                None,
            )

        logger.debug(f"Failed to fetch IMDb ID for ratingKey: {ratingKey}")

        return None

    def get_items_from_rss(self) -> list[tuple[str, str]]:
        """Fetch media from Plex RSS Feeds."""

        rss_items = list[tuple[str, str]]()

        if not self.rss_urls:
            logger.warning("No RSS URLs configured")
            return []

        for rss_url in self.rss_urls:
            try:
                response = self.session.get(rss_url + "?format=json", timeout=60)

                if not response.ok:
                    logger.error(f"Failed to fetch Plex RSS feed from {rss_url}")
                    continue

                class RSSResponseData(BaseModel):
                    class ItemModel(BaseModel):
                        category: Literal["movie", "show"]
                        title: str
                        guids: list[str]

                    items: list[ItemModel]

                data = RSSResponseData.model_validate(response.json())

                for item in data.items:
                    if item.category == "movie":
                        tmdb_id = next(
                            (
                                guid.split("//")[-1]
                                for guid in item.guids
                                if guid.startswith("tmdb://")
                            ),
                            "",
                        )

                        if tmdb_id:
                            rss_items.append(("movie", tmdb_id))
                        else:
                            logger.log(
                                "NOT_FOUND",
                                f"Failed to extract appropriate ID from {item.title}",
                            )

                    elif item.category == "show":
                        tvdb_id = next(
                            (
                                guid.split("//")[-1]
                                for guid in item.guids
                                if guid.startswith("tvdb://")
                            ),
                            "",
                        )

                        if tvdb_id:
                            rss_items.append(("show", tvdb_id))
                        else:
                            logger.log(
                                "NOT_FOUND",
                                f"Failed to extract appropriate ID from {item.title}",
                            )

            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while fetching Plex RSS feed from {rss_url}: {e}"
                )
        return rss_items

    def get_items_from_watchlist(self) -> list[dict[str, str | None]]:
        """Fetch media from Plex watchlist"""

        if not self.account:
            raise PlexAPIError("Plex account not authenticated")

        items = cast(list[Movie | Show], self.account.watchlist())
        watchlist_items = list[dict[str, str | None]]()

        for item in items:
            try:
                imdb_id: str | None = None
                tmdb_id: str | None = None
                tvdb_id: str | None = None

                if item.guids:
                    guids = [
                        GuidModel.model_validate(guid.__dict__)
                        for guid in cast(list[Guid], item.guids)
                    ]

                    imdb_id = next(
                        (
                            guid.id.split("//")[-1]
                            for guid in guids
                            if guid.id.startswith("imdb://")
                        ),
                        None,
                    )

                    if item.TYPE == "movie":
                        tmdb_id = next(
                            (
                                guid.id.split("//")[-1]
                                for guid in guids
                                if guid.id.startswith("tmdb://")
                            ),
                            "",
                        )
                    elif item.TYPE == "show":
                        tvdb_id = next(
                            (
                                guid.id.split("//")[-1]
                                for guid in guids
                                if guid.id.startswith("tvdb://")
                            ),
                            "",
                        )

                    if not any([imdb_id, tmdb_id, tvdb_id]):
                        logger.log(
                            "NOT_FOUND",
                            f"Unable to extract IMDb ID from {item.title} ({item.year}) with data id: {imdb_id}",
                        )
                        continue

                    watchlist_items.append(
                        {
                            "imdb_id": imdb_id,
                            "tmdb_id": tmdb_id,
                            "tvdb_id": tvdb_id,
                        }
                    )
                else:
                    logger.log(
                        "NOT_FOUND",
                        f"{item.title} ({item.year}) is missing guids attribute from Plex",
                    )
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while fetching Plex watchlist item {item.title}: {e}"
                )

        return watchlist_items

    def update_section(self, section: LibrarySection, path: str) -> bool:
        """Update the Plex section for the given path.

        Triggers a path-scoped scan, then blocks (bounded by
        SECTION_REFRESH_MAX_WAIT) until Plex reports the section is no longer
        refreshing. This applies backpressure so the single-threaded Updater
        paces completion bursts one scan at a time instead of firing overlapping
        scans that hammer the debrid VFS. See SECTION_REFRESH_* above.
        """
        try:
            section.update(str(path))
        except Exception as e:
            logger.error(f"Failed to update Plex section for path {path}: {e}")
            return False

        self._wait_for_section_idle(section)
        return True

    def _wait_for_section_idle(self, section: LibrarySection) -> None:
        """Poll until the section finishes its scan, capped at SECTION_REFRESH_MAX_WAIT.

        Best-effort: any polling failure is swallowed so it can never break the
        update path. An initial sleep gives Plex a moment to flip `refreshing`
        to True before we start checking, avoiding a premature exit.
        """
        waited = 0.0
        try:
            while waited < SECTION_REFRESH_MAX_WAIT:
                time.sleep(SECTION_REFRESH_POLL_INTERVAL)
                waited += SECTION_REFRESH_POLL_INTERVAL
                section.reload()
                if not getattr(section, "refreshing", False):
                    return
            logger.warning(
                f"Plex section '{getattr(section, 'title', '?')}' still refreshing "
                f"after {SECTION_REFRESH_MAX_WAIT:.0f}s; proceeding without waiting longer"
            )
        except Exception as e:
            logger.debug(f"Could not poll Plex section refresh state: {e}")

    def map_sections_with_paths(self) -> dict[LibrarySection, list[str]]:
        """Map Plex sections with their paths"""

        if not self.plex_server:
            raise PlexAPIError("Plex server not initialized")

        # Skip sections without locations and non-movie/show sections
        sections = [
            section
            for section in cast(
                list[LibrarySection],
                self.plex_server.library.sections(),
            )
            if section.type in ["show", "movie"] and section.locations
        ]
        # Map sections with their locations with the section obj as key and the location strings as values
        return {section: section.locations for section in sections}
