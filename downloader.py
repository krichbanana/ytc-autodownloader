#!/usr/bin/env python3
import sys
import os
import time
import subprocess
import signal
import datetime as dt
import json
import glob

from chat_downloader import ChatDownloader
from chat_downloader.sites import YouTubeChatDownloader
from chat_downloader.metadata import __version__ as CHATDOWNLOADER_VERSION
from chat_downloader.errors import (
    LoginRequired,
    VideoUnplayable,
    VideoUnavailable,
    ChatDisabled,
    NoChatReplay,
    ChatDownloaderError
)
from chat_downloader.utils.core import (
    safe_print
)

from utils import (
    create_file_lock,
    remove_file_lock,
    extract_video_id_from_yturl,
    get_utc_timestamp_now as get_timestamp_now
)

from notify import notify_send


try:
    from cookie_control import check_cookies_allowed
except ImportError:
    def check_cookies_allowed():
        global cookies_allowed
        cookies_allowed = True
        return cookies_allowed


# testing
ids = [
    'p1KolyCqICI',  # past broadcast
    'XWq5kBlakcQ',  # chat disabled
    'vprErlL1w2E',  # members only
    'xxxxxxxxxxx',  # removed/does not exist
    '5qap5aO4i9A',  # live video
]

EXIT_TRUE = 0
EXIT_FALSE = 1
EXIT_BADARG = 2


def compress_lzip(filename, nice=True):
    cmd = "lzip -9 --"
    if nice:
        cmd = 'nice ' + cmd
    cmd = cmd.split() + [filename]
    doneproc = subprocess.run(cmd)

    return doneproc.returncode


def compress_zstd(filename, nice=True):
    cmd = "zstd -f -19 --rm --"
    if nice:
        cmd = 'nice ' + cmd
    cmd = cmd.split() + [filename]
    doneproc = subprocess.run(cmd)

    return doneproc.returncode


def compress(filename):
    ret = None

    try:
        ret = compress_lzip(filename)
    except OSError:
        pass
    else:
        if ret != 0:
            print(f"(downloader) lzip compression failed: {ret = }", file=sys.stderr)
        else:
            print('(downloader) compression complete:', filename)

            return

    try:
        ret = compress_zstd(filename)
    except OSError:
        pass
    else:
        if ret != 0:
            print(f"(downloader) zstd compression failed: {ret = }", file=sys.stderr)
        else:
            print('(downloader) compression complete:', filename)

            return

    print(f"(downloader) compression failed: {filename = }", file=sys.stderr)


# status -> of a video's chat (detected external state); progress -> of the downloader (internal execution state)
# errors/retries are considered external factors, even if they aren't


cookies_allowed = False
cookies_warned = False


def try_for_cookies(video_id=None, channel_id=None, allow_generic=True):
    global cookies_warned
    # written this way to prioritize cookies/
    prefixes0 = ('', 'oo/', '../')
    prefixes = [x + "cookies/" for x in prefixes0]
    prefixes.extend(prefixes0)

    candidates = []
    if video_id is not None:
        for prefix in prefixes:
            candidates.append(prefix + video_id + ".txt")

    if channel_id is not None:
        for prefix in prefixes:
            candidates.append(prefix + channel_id + ".txt")

    if allow_generic is True:
        for prefix in prefixes:
            candidates.append(prefix + "cookies.txt")

    for path in candidates:
        if os.path.exists(path):
            if not cookies_allowed:
                if not cookies_warned:
                    print('warning: cookies forbidden, rejecting found cookies', file=sys.stderr)
                    cookies_warned = True

                return None

            return path

    return None


def _format_hms(curr_eta):
    # format as [[H?Hh:]M?Mm]S?S.FFFs
    neg = curr_eta < 0
    if neg:
        curr_eta *= -1
    curr_eta_hms = ["%d" % (curr_eta // 3600) + "h", ("%02d" % (curr_eta % 3600 // 60)) + "m", ("%06.3f" % (curr_eta % 60) + "s")]
    tmp_timestr = ''
    # strip leading zeros or the chunk entirely
    if curr_eta_hms[0] == '0h':
        curr_eta_hms[0] = None
    else:
        tmp_timestr = curr_eta_hms[0] + ':'

    if curr_eta_hms[0] is None:
        curr_eta_hms[1] = curr_eta_hms[1].removeprefix('0')
    if curr_eta_hms[1] == '0m':
        curr_eta_hms[1] = None
    else:
        tmp_timestr += curr_eta_hms[1] + ':'

    if curr_eta_hms[1] is None:
        curr_eta_hms[2] = curr_eta_hms[2].removeprefix('0')
    tmp_timestr += curr_eta_hms[2]

    if neg:
        tmp_timestr = '-' + tmp_timestr

    return tmp_timestr


class Downloader:
    def __init__(self, outname, video_id, init_timestamp_utc):
        self.outname = outname
        self.video_id = video_id
        self.init_timestamp_utc = init_timestamp_utc
        self.message_count = 0

    def write_current_progress(self, /, *, curr_status, curr_progress: str):
        """ Called during the download for key events """
        fd = None
        video_id = self.video_id
        try:
            fd = create_file_lock(f"by-video-id/{video_id}.lock")
            with open(f"by-video-id/{video_id}.dlprog", "a") as fp:
                curr_timestamp_utc = get_timestamp_now()
                res = {'init_timestamp_utc': self.init_timestamp_utc, 'curr_timestamp_utc': curr_timestamp_utc, 'curr_status': curr_status, 'curr_progress': curr_progress, 'outfile': self.outname, 'message_count': self.message_count, '_ts_version': 'utc'}
                fp.write(json.dumps(res))
        finally:
            if fd is not None:
                remove_file_lock(fd)

    def write_initial_progress(self, /, curr_progress: str):
        """ Called after the downloader has just spawned """
        fd = None
        video_id = self.video_id
        try:
            fd = create_file_lock(f"by-video-id/{video_id}.lock")
            with open(f"by-video-id/{video_id}.dlstart", "a") as fp:
                curr_timestamp_utc = get_timestamp_now()
                res = {'init_timestamp_utc': self.init_timestamp_utc, 'curr_timestamp_utc': curr_timestamp_utc, 'curr_progress': curr_progress, 'outfile': self.outname, 'downloader_version': CHATDOWNLOADER_VERSION, '_ts_version': 'utc'}
                fp.write(json.dumps(res))
        finally:
            if fd is not None:
                remove_file_lock(fd)

    def write_final_progress(self, /, exit_cause: str):
        """ Called when the downloader is about to exit """
        fd = None
        video_id = self.video_id
        try:
            fd = create_file_lock(f"by-video-id/{video_id}.lock")
            with open(f"by-video-id/{video_id}.dlend", "a") as fp:
                final_timestamp_utc = get_timestamp_now()
                res = {'init_timestamp_utc': self.init_timestamp_utc, 'final_timestamp_utc': final_timestamp_utc, 'exit_cause': exit_cause, 'outfile': self.outname, 'message_count': self.message_count, '_ts_version': 'utc'}
                fp.write(json.dumps(res))
        finally:
            if fd is not None:
                remove_file_lock(fd)

    def write_status(self, /, final_status: str):
        """ Called after the download has finished """
        fd = None
        video_id = self.video_id
        try:
            fd = create_file_lock(f"by-video-id/{video_id}.lock")
            with open(f"by-video-id/{video_id}.status", "a") as fp:
                final_timestamp_utc = get_timestamp_now()
                res = {'init_timestamp_utc': self.init_timestamp_utc, 'final_timestamp_utc': final_timestamp_utc, 'final_status': final_status, 'outfile': self.outname, '_ts_version': 'utc'}
                fp.write(json.dumps(res))
        finally:
            if fd is not None:
                remove_file_lock(fd)

    def run_loop(self, /):
        """ Download chat, retrying on errors """
        # control flow
        errors = 0
        retry = False
        paranoid_retry = False
        cookies = None
        new_cookies = False
        channel_id = None
        private = False
        # reporting only
        aborted = False
        started = False
        retried = False
        missed = False
        num_msgs = 0

        output_file = f"{self.outname}.json"
        max_retries = 720   # 12 hours, with 60 second delays

        old_video_id = self.video_id
        self.video_id = extract_video_id_from_yturl(self.video_id, strict=False)
        video_id = self.video_id
        if old_video_id != video_id:
            print('(downloader) warning: video_id was not a bare id.', file=sys.stderr)
        title = None
        author = None
        start_eta = old_start_eta = end_eta = None

        global cookies_allowed
        cookies_allowed = check_cookies_allowed()

        # Check for cookies at <video_id>.txt
        cookies = try_for_cookies(video_id=video_id, channel_id=None, allow_generic=False)
        if cookies is not None:
            print('(downloader) providing cookies since video-specific cookies present:', cookies)

        # Don't pass cookies if we don't have to.
        downloader = ChatDownloader(cookies=cookies)

        # Forcefully create a YouTube session
        youtube: YouTubeChatDownloader = downloader.create_session(YouTubeChatDownloader)

        last_progress = 'invoked'
        progress = 'invoked'
        ytstatus = ('unknown', 'unknown')

        try:
            fd = None
            try:
                # Try to throttle requests, especially on autoscraper startup
                prelocktime = get_timestamp_now()
                try:
                    fd = create_file_lock("webpage.lock")
                except KeyboardInterrupt:
                    print('warning: acquisition of lock was actually interrupted.')
                locktime = get_timestamp_now()
                lockdiff = locktime - prelocktime
                if lockdiff < 0:
                    print(f"warning: lock acquisition time is negative {lockdiff}; this doesn't make sense", file=sys.stderr)
                else:
                    print(f'notice: lock acquisition time was {lockdiff:0.3F}')
                details = youtube.get_video_data(video_id, params={'max_attempts': 7})
                yttime = get_timestamp_now()
                ytdiff = yttime - locktime
                time.sleep(0.5)
                if lockdiff < 0:
                    print(f"warning: yt details acquisition time is negative {ytdiff}; this doesn't make sense", file=sys.stderr)
                else:
                    print(f'notice: yt details acquisition time was {ytdiff:0.3F}')
            finally:
                if fd is not None:
                    remove_file_lock(fd)

            is_live = details.get('status') in {'live', 'upcoming'}
            channel_id = details.get('author_id')
            print('(downloader) initial:', details.get('status'), details.get('video_type'), video_id)
            title = details.get('title')
            print('(downloader) title:', title)
            author = details.get('author')
            print('(downloader) author:', author)
            if title is None:
                print('title missing, will dump video details')
                print('(downloader) video_id:', video_id)
                print('(downloader)', details)

            else:
                # chat_downloader converts it to microseconds for some reason
                start_eta = details.get('start_time')
                end_eta = details.get('end_time')
                if start_eta:
                    start_eta /= 1000000
                    old_start_eta = start_eta
                    if end_eta:
                        end_eta /= 1000000
                now_for_eta = float(dt.datetime.now().strftime("%s.%f"))
                timing_text = ''
                if start_eta:
                    if start_eta < now_for_eta:
                        curr_eta = -(now_for_eta - start_eta)
                    else:
                        curr_eta = start_eta - now_for_eta
                    if end_eta:
                        curr_eta_missed = -(now_for_eta - end_eta)

                if start_eta:
                    neg = curr_eta < 0
                    if neg:
                        curr_eta *= -1
                    tmp_timestr = _format_hms(curr_eta)

                    if end_eta:
                        tmp_timestr_missed = _format_hms(curr_eta_missed)
                        timing_text = f'Live ended at {tmp_timestr_missed} ago; stream began {tmp_timestr} ago'
                    elif neg:
                        timing_text = f'Live started {tmp_timestr} ago'
                    else:
                        timing_text = f'Live set to start in +{tmp_timestr}'
                else:
                    timing_text = 'Live is late?!'
                    if old_start_eta:
                        tmp_timestr = _format_hms(now_for_eta - old_start_eta)
                        timing_text += f' Expected {tmp_timestr} ago'
                    else:
                        timing_text += ' Expected who knows how long ago, hopefully recently'

                print('(downloader)', timing_text)

                ytstatus = details.get('status'), details.get('video_type')
                progress = 'data-fetched'
                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                last_progress = progress

            # No continuation? Possibly members-only.
            if details.get('continuation_info') == {} and cookies is None:
                cookies = try_for_cookies(video_id=video_id, channel_id=channel_id)
                if cookies is not None:
                    print('(downloader) providing cookies since chat is missing:', cookies)
                    downloader = ChatDownloader(cookies=cookies)
                    youtube = downloader.create_session(YouTubeChatDownloader)

        except AttributeError:
            print('(downloader) warning: chat_downloader out of date.', file=sys.stderr)
            details = None
            is_live = None

        try:
            currtime = dt.datetime.now()
            lasttime = currtime
            lastmsgcount = 0
            while True:
                currtime = dt.datetime.now()
                delta = currtime - lasttime
                if delta.seconds > 90:
                    print('(downloader) warning: video %s: download loop stuck for %d.%06d seconds' % (video_id, delta.seconds, delta.microseconds), file=sys.stderr)
                lasttime = currtime
                if retry or paranoid_retry:
                    if not paranoid_retry:
                        errors += 1

                        if errors == 1:
                            print('(downloader) Waiting 60 seconds before retrying:', video_id)
                            notify_send('downloader waiting', f'{video_id} ({ytstatus[0]} {ytstatus[1]}): {title} / {author} / {timing_text} (cached)', timeout_msec=15000)

                        elif errors > max_retries:
                            print('(downloader) Retry limit reached:', video_id)
                            aborted = True

                            break
                    else:
                        progress = 'paranoid-retry'
                        self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                        last_progress = progress

                    # Retry members only video on new cookies immediately.
                    if not new_cookies and (not paranoid_retry or private):
                        time.sleep(60)
                    else:
                        new_cookies = False

                    fd = None
                    try:
                        if not started and private and not new_cookies:
                            # Throttle time-waiting inaccessible video tasks to avoid hammering YouTube
                            fd = create_file_lock("private.lock")
                            time.sleep(5)
                        else:
                            # Excessive parallelism may trigger YouTube's anti-bot mechanisms
                            prelocktime = get_timestamp_now()
                            fd = create_file_lock("webpage.lock")
                            locktime = get_timestamp_now()
                            time.sleep(0.5)
                            lockdiff = locktime - prelocktime
                            if lockdiff < 0:
                                print(f"warning: lock acquisition time is negative {lockdiff}; this doesn't make sense", file=sys.stderr)
                            else:
                                print(f'notice: lock acquisition time was {lockdiff:0.3F}')

                        downloader = ChatDownloader(cookies=cookies)

                        youtube: YouTubeChatDownloader = downloader.create_session(YouTubeChatDownloader)

                        details = youtube.get_video_data(video_id, params={'max_attempts': 7})
                        is_live = details.get('status') in {'live', 'upcoming'}
                        title = details.get('title')
                        author = details.get('author')
                        if retry:
                            print('(downloader) retry:', details.get('status'), details.get('video_type'), video_id)
                            if progress != last_progress:
                                # Don't compare this 'progress' at all, just for event logging. Else, we log each time... we may remove logging for this message in the future.
                                progress = 'data-refetched'
                                ytstatus = details.get('status'), details.get('video_type')
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                # don't update last_progress so that the body and exceptions can do that.
                        else:
                            print('(downloader) retry (paranoid):', details.get('status'), details.get('video_type'), video_id)

                    except AttributeError:
                        print('AttributeError', video_id, f"{details = }")
                        details = None
                        is_live = None

                    finally:
                        if fd is not None:
                            remove_file_lock(fd)

                retry = True
                timing_text = ''

                # chat_downloader converts it to microseconds for some reason
                start_eta = details.get('start_time')
                end_eta = details.get('end_time')
                if start_eta:
                    start_eta /= 1000000
                    old_start_eta = start_eta
                    if end_eta:
                        end_eta /= 1000000
                now_for_eta = float(dt.datetime.now().strftime("%s.%f"))
                if start_eta:
                    if start_eta < now_for_eta:
                        curr_eta = -(now_for_eta - start_eta)
                    else:
                        curr_eta = start_eta - now_for_eta
                    if end_eta:
                        curr_eta_missed = -(now_for_eta - end_eta)

                if start_eta:
                    neg = curr_eta < 0
                    if neg:
                        curr_eta *= -1
                    tmp_timestr = _format_hms(curr_eta)

                    if end_eta:
                        tmp_timestr_missed = _format_hms(curr_eta_missed)
                        timing_text = f'Live ended {tmp_timestr_missed} ago; stream began {tmp_timestr} ago'
                    elif neg:
                        timing_text = f'Live started {tmp_timestr} ago'
                    else:
                        timing_text = f'Live set to start in +{tmp_timestr}'
                else:
                    timing_text = 'Live is late?!'
                    if old_start_eta:
                        tmp_timestr = _format_hms(now_for_eta - old_start_eta)
                        timing_text += f' Expected {tmp_timestr} ago'
                    else:
                        timing_text += ' Expected who knows how long ago, hopefully recently'

                try:
                    chat = downloader.get_chat(video_id, output=output_file, message_groups=['all'], indent=2, overwrite=False, interruptible_retry=False)

                    private = False

                    # chat_downloader feature check
                    try:
                        is_live = chat.is_live
                    except AttributeError:
                        pass

                    if is_live:
                        if not started:
                            progress = 'chat-download-started'
                            if progress != last_progress:
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                last_progress = progress
                        else:
                            progress = 'chat-download-restarted'
                            if progress != last_progress:
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                last_progress = progress

                        started = True

                        print('(downloader) Downloading live chat from video:', video_id)
                        if paranoid_retry:
                            print("(downloader) warning: chat downloader exited too soon!", video_id, f"{num_msgs = }", file=sys.stderr)
                            try:
                                notify_send('restarted downloader', f'{video_id} ({ytstatus[0]} {ytstatus[1]}): {title} / {author} / {timing_text}', timeout_msec=60000)
                            except IndexError:
                                print('ytstatus bug')
                            retried = True
                            paranoid_retry = False
                        else:
                            try:
                                notify_send('started downloader', f'{video_id} ({ytstatus[0]} {ytstatus[1]}): {title} / {author} / {timing_text}', timeout_msec=15000)
                            except IndexError:
                                print('ytstatus bug')

                        with open(f"{self.outname}.stdout", "a") as fp:
                            for message in chat:                        # iterate over messages
                                num_msgs += 1
                                self.message_count = num_msgs
                                # print the formatted message
                                safe_print(chat.format(message), out=fp)
                                fp.flush()
                                # performance check
                                currtime = dt.datetime.now()
                                delta = currtime - lasttime
                                msgdelta = num_msgs - lastmsgcount
                                if delta.seconds > 90 and msgdelta > 1:
                                    print('(downloader) warning: video %s: chat iteration stuck for %d.%06d seconds (got %d/%d new/total messages)' % (video_id, delta.seconds, delta.microseconds, msgdelta, num_msgs), file=sys.stderr)
                                    notify_send('stuck iteration', f'{video_id}', timeout_msec=15000)
                                lasttime = currtime
                                lastmsgcount = num_msgs

                        # finished... maybe? we should retry anyway.
                        paranoid_retry = True

                    else:
                        if not paranoid_retry:
                            print('(downloader) Video is not live, ignoring:', video_id)
                            missed = True

                            progress = 'exiting:missed'
                            if progress != last_progress:
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                last_progress = progress

                        paranoid_retry = False

                        break

                except LoginRequired:
                    if not paranoid_retry:
                        print('(downloader) Private video detected:', video_id)
                        private = True
                        progress = 'retrywait:private'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    else:
                        break

                except VideoUnplayable:
                    # YouTube should provide is_live, even on subscriber_only (member) videos
                    if details and details.get('status') is not None:   # feature check
                        if not is_live:
                            # members-only stream, missed.
                            print('(downloader) Member video is not live, ignoring:', video_id)
                            # NOTE: there's an uncommon case where chat goes member's only after a video ends.
                            # In this case we will lose the last few messages as we weren't initially auth'd
                            # with cookies, and the cookies won't get used if we just ended.
                            # One solution could be to pass cookies to every vid, but that can easily be tracked.
                            missed = True

                            progress = 'exiting:member'
                            if progress != last_progress:
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                last_progress = progress

                            break

                        else:
                            progress = 'retrywait:member'
                            if progress != last_progress:
                                self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                                last_progress = progress

                    next_cookies = try_for_cookies(video_id=video_id, channel_id=channel_id)
                    new_cookies = False
                    if next_cookies is not None:
                        if cookies is None:
                            print('(downloader) found new cookies:', video_id)
                            cookies = next_cookies
                            new_cookies = True
                        else:
                            if cookies != next_cookies:
                                print('(downloader) found new cookies, different from current cookies:', video_id)
                                cookies = next_cookies
                                new_cookies = True

                    if not new_cookies:
                        if cookies is not None:
                            print('(downloader) Members only video detected, try again with different cookies:', video_id)
                        else:
                            print('(downloader) Members only video detected, try again with cookies:', video_id)
                            # Use private lock queue for member streams, since without cookies they are unlikely to (re-)start
                            private = True
                    else:
                        print('(downloader) Members only video detected, will try again with new cookies:', video_id)

                except VideoUnavailable as e:
                    print('(downloader) Removed video detected, giving up:', video_id, f'(message: {e.args[0]})')

                    if not paranoid_retry:
                        aborted = True

                        progress = 'abort:removal'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    else:
                        progress = 'exiting:removal'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    break

                except ChatDisabled:
                    if not paranoid_retry:
                        print('(downloader) Disabled chat detected:', video_id)
                        # 1 week, 60 second intervals (might be longer if videos are rescheduled)
                        max_retries = max(max_retries, 10080)

                        progress = 'retryloop:disabled'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    else:
                        # we should reach "NoChatReplay" instead
                        progress = 'exiting:disabled'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                        break

                except NoChatReplay:
                    if not paranoid_retry:
                        print('(downloader) Video is not live, ignoring (no replay):', video_id)
                        missed = True

                        progress = 'abort:noreplay'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    else:
                        progress = 'exiting:noreplay'
                        if progress != last_progress:
                            self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                            last_progress = progress

                    break

                except ChatDownloaderError as e:
                    print(f'(downloader) {e}:', video_id)
                    # 1 day, 60 second intervals
                    max_retries = max(max_retries, 1440)

                    progress = 'downloader-error'
                    if progress != last_progress:
                        self.write_current_progress(curr_status=ytstatus, curr_progress=progress)
                        last_progress = progress

                else:
                    retry = False

        except KeyboardInterrupt:
            print('(downloader) warning: got sigint, cancelling download:', self.video_id, file=sys.stderr)
            aborted = True

        if aborted or errors > max_retries:
            print('(downloader) warning: download incomplete:', self.video_id, file=sys.stderr)

            if started:
                if retried:
                    self.write_status('started+retried+aborted')
                else:
                    self.write_status('started+aborted')
            else:
                self.write_status('aborted')

        elif not started and missed:
            print('(downloader) download missed:', self.video_id)

            if retried:
                self.write_status('missed+retried')
            else:
                self.write_status('missed')

        else:
            print('(downloader) download complete:', self.video_id)

            if retried:
                self.write_status('finished+retried')
            else:
                self.write_status('finished')

        print('(downloader) download stats:', num_msgs, "messages for video", self.video_id)

        try:
            # Excessive parallelism may trigger YouTube's anti-bot mechanisms
            prelocktime = get_timestamp_now()
            fd = create_file_lock("webpage.lock")
            locktime = get_timestamp_now()
            details = youtube.get_video_data(video_id, params={'max_attempts': 7})
            timing_text = ''

            # chat_downloader converts it to microseconds for some reason
            start_eta = details.get('start_time')
            old_end_eta = end_eta
            end_eta = details.get('end_time')
            if start_eta:
                start_eta /= 1000000
                old_start_eta = start_eta
                if end_eta:
                    end_eta /= 1000000
            now_for_eta = float(dt.datetime.now().strftime("%s.%f"))
            if start_eta:
                if start_eta < now_for_eta:
                    curr_eta = -(now_for_eta - start_eta)
                else:
                    curr_eta = start_eta - now_for_eta
                if end_eta:
                    curr_eta_missed = -(now_for_eta - end_eta)
                    # A live that is marked finished should close live chat after 5 minutes
                    if abs(curr_eta_missed) > 360:
                        timing_text += '(unusually delayed exit) '
            elif old_start_eta:
                timing_text += '(private now?) '
                curr_eta = now_for_eta - old_start_eta
                if old_end_eta:
                    curr_eta_missed = now_for_eta - old_end_eta

            if start_eta:
                neg = curr_eta < 0
                if neg:
                    curr_eta *= -1
                tmp_timestr = _format_hms(curr_eta)

                if end_eta:
                    tmp_timestr_missed = _format_hms(curr_eta_missed)
                    timing_text = f'Live ended {tmp_timestr_missed} ago; stream began {tmp_timestr} ago'
                elif neg:
                    print(start_eta, now_for_eta)
                    timing_text = f'Live started {tmp_timestr} ago'
                else:
                    timing_text = f'Live set to start in +{tmp_timestr}'
            else:
                timing_text = 'Live is late?!'
                if old_start_eta:
                    tmp_timestr = _format_hms(now_for_eta - old_start_eta)
                    timing_text += f' Expected {tmp_timestr} ago'
                else:
                    timing_text += ' Expected who knows how long ago, hopefully recently'

            print(f'(downloader) download exit analysis for video {self.video_id}: {progress} / {timing_text}')

            time.sleep(0.5)
            lockdiff = locktime - prelocktime
            if lockdiff < 0:
                print(f"warning: lock acquisition time is negative {lockdiff}; this doesn't make sense", file=sys.stderr)
            else:
                print(f'notice: lock acquisition time was {lockdiff:0.3F}')

        except RuntimeError:
            print('(downloader) post-processing exception occured, likely video data fetch issue')

        finally:
            if fd is not None:
                remove_file_lock(fd)

        # Compress logs after the downloader exits.
        if started:
            # Throttle compress tasks to avoid stacking CPU and memory usage
            fd = create_file_lock("compress.lock")
            for ext in ['.json', '.stdout']:
                compress(self.outname + ext)
            remove_file_lock(fd)


def handle_special_signal(signum, frame):
    # TODO
    pass


def compress_ytdlp(video_id):
    fd = create_file_lock("compress.lock")
    globber = '_' + video_id + '_*.ytdlp.*'
    for file in glob.iglob(globber):
        if file.split('.')[-1] in ['lz', 'zst']:
            pass
        compress(file)
    remove_file_lock(fd)


def main():
    outname = sys.argv[1]
    video_id = sys.argv[2]
    if len(sys.argv) == 3:
        signal.signal(signal.SIGUSR1, handle_special_signal)
        init_timestamp_utc = get_timestamp_now()
        downloader = Downloader("chat-logs/" + outname, video_id, init_timestamp_utc)
        try:
            downloader.write_initial_progress('invoked')
            notify_send('downloader invoked', f'{video_id}', timeout_msec=5000)
            downloader.run_loop()
        except Exception as e:
            try:
                downloader.write_final_progress('crashed')
            except FileNotFoundError:
                print('(downloader) state directory is not set up!', file=sys.stderr)
            print(f"(downloader) fatal exception (pid = {os.getpid()}, ppid = {os.getppid()}, video_id = {downloader.video_id}, outname = {downloader.outname})")
            notify_send('downloader died :(', f'{video_id}', timeout_msec=60000)
            downloader = Downloader("chat-logs/" + outname, video_id, init_timestamp_utc)
            downloader.write_initial_progress('invoked-yt-dlp')
            subprocess.run(['yt-dlp', '--skip-download', '--all-subs', '--ignore-no-formats-error', '--sub-langs', 'live_chat', '-o', '_%(id)s_start-%(release_timestamp)s.ytdlp', '--', video_id])
            try:
                compress_ytdlp(video_id)
            except OSError:
                pass
            downloader.write_final_progress('exited-yt-dlp')
            notify_send('downloader finished (yt-dlp)', f'{video_id}', timeout_msec=10000)
            raise e  # exit with non-zero status
        else:
            downloader.write_final_progress('finished')
            notify_send('downloader finished', f'{video_id}', timeout_msec=10000)

    else:
        print("usage: {} '<outname>' '<video ID>'".format(sys.argv[0]))
        sys.exit(EXIT_BADARG)


if __name__ == '__main__':
    main()
