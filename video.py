#!/usr/bin/env python3
import sys
from utils import get_timestamp_now

statuses = frozenset(['unknown', 'prelive', 'live', 'postlive', 'upload', 'error'])
progress_statuses = frozenset(['unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'])
write_counter_values = frozenset(['basic', 'metafile', 'status_metafile'])


class TransitionException(Exception):
    """ Invalid live status transition by setter """
    pass


class BaseVideo:
    """ Record the online status of a video, along with the scraper's download stage.
        Metadata from Youtube is also stored when needed.
        video_id is the unique Youtube ID for identifying a video.
    """
    def __init__(self, video_id, *, id_source=None, referrer_channel_id=None):
        self.video_id = video_id
        self.id_source = id_source
        self.referrer_channel_id = referrer_channel_id
        self.status = 'unknown'
        self.progress = 'unscraped'
        self.warned = False
        self.init_timestamp = get_timestamp_now()
        self.transition_timestamp = self.init_timestamp
        self.meta_timestamp = None
        # might delete one
        self.meta = None
        self.rawmeta = None
        # might change
        self.did_discovery_print = False
        self.did_status_print = False
        self.did_progress_print = False
        self.did_meta_flush = False
        self.status_flush_reason = 'new video object'
        self.progress_flush_reason = 'new video object'
        self.meta_flush_reason = 'new video object'
        # declare our possible intent here
        self.next_event_check = 0
        self.create_counter = {}
        self.overwrite_counter = {}

    def set_status(self, status: str):
        """ Set the online status (live progress) of a video
            Currently can be any of: 'unknown', 'prelive', 'live', 'postlive', 'upload'.
            Invalid progress transtitions print a warning (except for 'unknown').
        """
        if status not in statuses:
            raise ValueError(f"tried to set invalid status: {status}")

        if status == 'unknown':
            raise TransitionException("status cannot be set to 'unknown', only using reset")

        if status == 'prelive' and self.status in {'live', 'postlive', 'upload'} \
                or status == 'live' and self.status in {'postlive', 'upload'} \
                or status == 'postlive' and self.status in {'upload'}:
            print(f"warning: new video status invalid: transitioned from {self.status} to {status}", file=sys.stderr)
            self.warned = True

        if status == 'postlive' and self.status in {'prelive'}:
            print(f"warning: new video status suspicious: transitioned from {self.status} to {status}", file=sys.stderr)
            self.warned = True

        self.status_flush_reason = getattr(self, 'status_flush_reason', 'new video object (outdated layout!)')

        if status == self.status:
            print(f"warning: new video status suspicious: no change in status: {status}", file=sys.stderr)
            self.warned = True
        else:
            if self.did_status_print:
                self.status_flush_reason = f'status changed: {self.status} -> {status}'
            else:
                self.status_flush_reason += f'; {self.status} -> {status}'
            self.did_status_print = False

        self.transition_timestamp = get_timestamp_now()
        self.status = status

    def set_progress(self, progress: str):
        """ Set the scraper progress of a video
            Currently can be any of: 'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'
            Invalid progress transtitions throw a TransitionException.
        """
        if progress not in progress_statuses:
            raise ValueError(f"tried to set invalid progress status: {progress}")

        if progress == 'unscraped':
            raise TransitionException("progress cannot be set to 'unscraped', only using reset")

        if progress == 'waiting' and self.progress != 'unscraped' \
                or progress == 'downloading' and self.progress != 'waiting' \
                or progress == 'downloaded' and self.progress != 'downloading' \
                or progress == 'missed' and self.progress not in {'unscraped', 'waiting'} \
                or progress == 'invalid' and self.progress != 'unscraped' \
                or progress == 'aborted' and self.progress == 'downloaded':
            raise TransitionException(f"progress cannot be set to {progress} from {self.progress}")

        self.progress_flush_reason = getattr(self, 'progress_flush_reason', 'new video object (outdated layout!)')

        if progress == self.progress:
            print(f"warning: new progress status suspicious: no change in progress: {progress}", file=sys.stderr)
            self.warned = True
        else:
            if self.did_progress_print:
                self.progress_flush_reason = f'progress changed: {self.progress} -> {progress}'
            else:
                self.progress_flush_reason += f'; {self.progress} -> {progress}'
            self.did_progress_print = False

        self.transition_timestamp = get_timestamp_now()
        self.progress = progress

        if progress in {'unscraped', 'waiting', 'downloading'} and self.status == 'postlive':
            print(f"warning: overriding new progress state due to postlive status: {progress} -> missed", file=sys.stderr)
            self.progress = 'missed'

    def reset_status(self):
        """ Set the status to 'unknown'. Useful for clearing state loaded from disk. """
        self.did_status_print = False
        self.status_flush_reason = f'status reset: {self.status} -> unknown'
        self.status = 'unknown'

    def reset_progress(self):
        """ Set progress to 'unscraped'. Useful for clearing state loaded from disk. """
        self.did_progress_print = False
        self.progress_flush_reason = f'progress reset: {self.progress} -> unscraped'
        self.progress = 'unscraped'

    def _update_create_counter(self, name: str):
        """ debug counter to check meta creation counts """
        self.create_counter = getattr(self, 'create_counter', {})
        if name not in write_counter_values:
            print(f'warning: unknown debug create counter name: {name}', file=sys.stderr)
        self.create_counter[name] = self.create_counter.setdefault(name, 0) + 1

    def _update_overwrite_counter(self, name: str):
        """ debug counter to check meta overwrite (clobber) counts """
        self.overwrite_counter = getattr(self, 'overwrite_counter', {})
        if name not in write_counter_values:
            print(f'warning: unknown debug overwrite counter name: {name}', file=sys.stderr)
        self.overwrite_counter[name] = self.overwrite_counter.setdefault(name, 0) + 1
