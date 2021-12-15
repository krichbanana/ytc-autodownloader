#!/usr/bin/env python3
import sys
from utils import get_timestamp_now
from video import (
    BaseVideo,
    TransitionException
)

class BaseChannel:
    """ Tracks basic details about a channel, such as the videos that belong to it. """
    def __init__(self, channel_id):
        self.channel_id = channel_id
        self.videos = set()
        self.init_timestamp = get_timestamp_now()
        self.modify_timestamp = self.init_timestamp
        self.batch_end_timestamp = 0
        self.did_discovery_print = False
        self.batching = False
        self.batch = None

    def add_video(self, video: BaseVideo):
        """ Add a video to our list, and possibly our current batch
            Modifies timestamp on success
        """
        if video.video_id not in self.videos:
            self.videos.add(video.video_id)
            self.modify_timestamp = get_timestamp_now()
            self.did_discovery_print = False
            if self.batching:
                self.batch.add(video.video_id)

    def add_video_ids(self, video_ids: list):
        """ Add videos to our list, and possibly our current batch
            Modifies timestamp on success
        """
        new_videos = set(video_ids) - self.videos
        if len(new_videos) > 0:
            self.modify_timestamp = get_timestamp_now()
            self.did_discovery_print = False
            if self.batching:
                self.batch |= new_videos

    # A batch is used to identify the results of the last scrape task.

    def start_batch(self):
        """ Declare that the next videos are a new batch, resetting batch state. """
        if self.batching:
            raise TransitionException("channel batch already started")

        self.batching = True
        self.batch_end_timestamp = 0
        self.batch = set()

    def end_batch(self):
        """ Finish declaring that the next videos are a new batch
            Mark the timestamp for when this occured.
        """
        if not self.batching:
            raise TransitionException("channel batch not started")

        self.batch_end_timestamp = get_timestamp_now()
        self.batching = False

    def clear_batch(self):
        """ Forget a batch (does not affect list of videos)
            Forget the timestamp for when a batch was finished.
        """
        self.batching = False
        self.batch_end_timestamp = 0
        self.batch = set()
