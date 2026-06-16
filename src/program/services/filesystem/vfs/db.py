from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from kink import di
from loguru import logger

from pydantic import BaseModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from program.db.db import db_session
from program.media.media_entry import MediaEntry
from program.services.streaming.exceptions import (
    DebridServiceLinkUnavailable,
)
from program.media.item import MediaItem
from program.types import Event
from routers.secure.items import apply_item_mutation
from program.utils.debrid_cdn_url import DebridCDNUrl

if TYPE_CHECKING:
    from program.services.downloaders import Downloader


class VFSEntry(TypedDict):
    virtual_path: str
    name: str
    size: int
    is_directory: bool
    entry_type: str | None
    created: str | None
    modified: str | None


class GetEntryByOriginalFilenameResult(BaseModel):
    original_filename: str
    download_url: str | None
    unrestricted_url: str | None
    provider: str | None
    provider_download_id: str | None
    size: int | None
    created: str | None
    modified: str | None
    entry_type: Literal["media", "subtitle"]

    @property
    def url(self) -> str | None:
        """The URL to use for this request."""

        return self.unrestricted_url or self.download_url


class VFSDatabase:
    def __init__(self, downloader: "Downloader | None" = None) -> None:
        """
        Initialize VFS Database.

        Args:
            downloader: Downloader instance with initialized services for URL resolution
        """

        self.downloader = downloader

    # --- Queries ---
    def get_subtitle_content(
        self,
        parent_original_filename: str,
        language: str,
    ) -> bytes | None:
        """
        Get the subtitle content for a SubtitleEntry.

        In the new architecture, subtitles are looked up by their parent video's
        original_filename and language code, not by path.

        Parameters:
            parent_original_filename (str): Original filename of the parent MediaEntry (video file).
            language (str): ISO 639-3 language code (e.g., 'eng').

        Returns:
            bytes: Subtitle content encoded as UTF-8, or None if not found or not a subtitle.
        """

        with db_session() as session:
            from program.media.subtitle_entry import SubtitleEntry

            # Query specifically for SubtitleEntry by parent and language
            subtitle = (
                session.query(SubtitleEntry)
                .filter_by(
                    parent_original_filename=parent_original_filename, language=language
                )
                .first()
            )

            if subtitle and subtitle.content:
                return subtitle.content.encode("utf-8")

            return None

    def refresh_unrestricted_url(
        self,
        entry: MediaEntry,
        session: Session,
    ) -> str | None:
        """
        Refresh the unrestricted URL for a MediaEntry using the downloader services.

        Args:
            entry: MediaEntry to refresh
        """

        if not self.downloader:
            logger.warning("No downloader available to refresh unrestricted URL")

            return None

        from program.program import Program

        # Find service by matching the key attribute (services dict uses class as key)
        service = next(
            (
                svc
                for svc in self.downloader.services.values()
                if svc.key == entry.provider
            ),
            None,
        )

        if service and entry.download_url:
            try:
                new_unrestricted = service.unrestrict_link(entry.download_url)

                if new_unrestricted:
                    entry.unrestricted_url = new_unrestricted.download

                    cdn_url = DebridCDNUrl(entry)

                    if cdn_url.validate(attempt_refresh=False):
                        session.merge(entry)
                        session.commit()

                        logger.debug(
                            f"Refreshed unrestricted URL for {entry.original_filename}"
                        )

                        return entry.unrestricted_url
            except DebridServiceLinkUnavailable as e:
                logger.warning(
                    f"Failed to unrestrict URL for {entry.original_filename}: {e}"
                )

                # A dead/evicted torrent is usually a season pack shared by many media items.
                # On the FIRST detection (the downloader flags the torrent and reports it via the
                # exception), reset every sibling sharing that torrent in one pass so the whole
                # pack blacklists + re-scrapes together instead of one item per VFS read.
                # Otherwise fall back to resetting just this item.
                if self._reset_items_for_dead_link(entry, session, e):
                    return None
                raise
            except Exception as e:
                logger.warning(
                    f"Unexpected error when unrestricting URL for {entry.original_filename}: {e}"
                )

        return None

    def _reset_items_for_dead_link(
        self,
        entry: MediaEntry,
        session: Session,
        error: DebridServiceLinkUnavailable,
    ) -> bool:
        """
        Reset the MediaItem(s) affected by a dead debrid link so they re-scrape.

        When the link failure is the first detection of a dead torrent (a shared season pack),
        every MediaEntry pointing at the same provider torrent is reset together in a single
        pass; siblings then collapse at once rather than one-per-VFS-read. Otherwise only this
        entry's item is reset.

        Each affected item is blacklisted (so the dead pack is not re-picked, which would also
        waste a createtorrent token) and reset (so it re-scrapes to a different source).

        Returns:
            True if at least one item was reset; False if there was no item to reset (caller
            should re-raise so the failure is not silently swallowed).
        """

        from program.program import Program

        # torrent_id is carried on the exception; the provider torrent on the MediaEntry is
        # stored as provider_download_id. Prefer the column for the sibling lookup.
        torrent_id = getattr(error, "torrent_id", None)
        first_detection = getattr(error, "first_detection", False)

        items: list[MediaItem] = []

        if (
            first_detection
            and entry.provider
            and entry.provider_download_id
        ):
            # Reset all siblings sharing this exact provider torrent.
            sibling_entries = (
                session.query(MediaEntry)
                .filter(
                    MediaEntry.provider == entry.provider,
                    MediaEntry.provider_download_id == entry.provider_download_id,
                )
                .all()
            )

            seen: set = set()

            for sibling in sibling_entries:
                item = sibling.media_item

                if item is not None and item.id not in seen:
                    seen.add(item.id)
                    items.append(item)

            logger.warning(
                f"Dead TorBox torrent {torrent_id or entry.provider_download_id}: resetting "
                f"{len(items)} item(s) sharing the pack so they re-scrape together"
            )
        elif entry.media_item is not None:
            items = [entry.media_item]

        if not items:
            return False

        def mutation(i: MediaItem, s: Session):
            i.blacklist_active_stream()
            i.reset()

        # Reset each item inside its own SAVEPOINT. A sibling can have a job (e.g. a Downloader
        # event from the startup backlog) still running despite cancel_job -- its concurrent
        # StreamRelation delete races ours and raises StaleDataError ("expected to delete 1
        # row(s); Only 0 were matched"). With a single batch commit, one such race rolls back the
        # whole pack and poisons the session (PendingRollbackError). The savepoint contains the
        # failure to just that item: it is skipped (the row is gone either way) and gets retried
        # via its normal event flow, while the rest of the pack still collapses.
        succeeded_ids: list[int] = []

        for item in items:
            item_id = item.id

            try:
                with session.begin_nested():
                    apply_item_mutation(
                        program=di[Program],
                        item=item,
                        mutation_fn=mutation,
                        session=session,
                    )

                succeeded_ids.append(item_id)
            except SQLAlchemyError as e:
                # Savepoint already rolled back by the context manager; outer txn stays usable.
                logger.warning(
                    f"Skipped resetting item {item_id} for dead torrent "
                    f"{torrent_id or entry.provider_download_id} "
                    f"(concurrent stream mutation): {e}"
                )

        if not succeeded_ids:
            # Every item raced; nothing committed. Let the caller re-raise so the read fails
            # cleanly and the items retry, rather than reporting a successful reset.
            return False

        session.commit()

        for item_id in succeeded_ids:
            di[Program].em.add_event(Event("VFS", item_id))

        return True

    def get_entry_by_original_filename(
        self,
        original_filename: str,
        force_resolve: bool = False,
    ) -> GetEntryByOriginalFilenameResult | None:
        """
        Get entry metadata and download URL by original filename.

        This is the NEW API that replaces path-based lookups.

        Args:
            original_filename: Original filename from debrid provider
            force_resolve: If True, force refresh of unrestricted URL from provider

        Returns:
            Dictionary with entry metadata and URLs, or None if not found
        """

        try:
            with db_session() as session:
                entry = (
                    session.query(MediaEntry)
                    .filter(MediaEntry.original_filename == original_filename)
                    .first()
                )

                if not entry:
                    return None

                # Get download URL (with optional unrestricting)
                download_url = entry.download_url
                unrestricted_url = entry.unrestricted_url

                # If force_resolve or no unrestricted URL, try to unrestrict
                if (force_resolve or not unrestricted_url) and (
                    self.downloader and entry.provider
                ):
                    unrestricted_url = self.refresh_unrestricted_url(
                        entry,
                        session=session,
                    )

                return GetEntryByOriginalFilenameResult(
                    original_filename=entry.original_filename,
                    download_url=download_url,
                    unrestricted_url=unrestricted_url,
                    provider=entry.provider,
                    provider_download_id=entry.provider_download_id,
                    size=entry.file_size,
                    created=(entry.created_at.isoformat()),
                    modified=(entry.updated_at.isoformat()),
                    entry_type="media",
                )
        except DebridServiceLinkUnavailable:
            raise
        except Exception as e:
            logger.error(
                f"Error getting entry by original_filename {original_filename}: {e}"
            )
            return None
