#!/usr/bin/env python3
import sys
import os
import time
import subprocess
import signal
import datetime as dt
import json
import traceback

from chat_downloader import ChatDownloader
from chat_downloader.sites import YouTubeChatDownloader
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
    extract_video_id_from_yturl
)


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


def compress_lzip(filename):
    cmd = "lzip -9".split() + [filename]
    doneproc = subprocess.run(cmd)
    return doneproc.returncode


def compress_zstd(filename):
    cmd = "zstd -f -19 --rm".split() + [filename]
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


def write_current_progress(curr_status: str, curr_progress: str, video_id: str, init_timestamp, outfile: str):
    """ Called during the download for key events """
    fd = None
    try:
        fd = create_file_lock(f"by-video-id/{video_id}.lock")
        with open(f"by-video-id/{video_id}.dlprog", "a") as fp:
            curr_timestamp = dt.datetime.utcnow().timestamp()
            res = {'init_timestamp': init_timestamp, 'curr_timestamp': curr_timestamp, 'curr_status': curr_status, 'curr_progress': curr_progress, 'outfile': outfile}
            fp.write(json.dumps(res))
    finally:
        if fd is not None:
            remove_file_lock(fd)


def write_initial_progress(curr_progress: str, video_id: str, init_timestamp, outfile: str):
    """ Called after the downloader has just spawned """
    fd = None
    try:
        fd = create_file_lock(f"by-video-id/{video_id}.lock")
        with open(f"by-video-id/{video_id}.dlstart", "a") as fp:
            curr_timestamp = dt.datetime.utcnow().timestamp()
            res = {'init_timestamp': init_timestamp, 'curr_timestamp': curr_timestamp, 'curr_progress': curr_progress, 'outfile': outfile}
            fp.write(json.dumps(res))
    finally:
        if fd is not None:
            remove_file_lock(fd)


def write_final_progress(exit_cause: str, video_id: str, init_timestamp, outfile: str):
    """ Called when the downloader is about to exit """
    fd = None
    try:
        fd = create_file_lock(f"by-video-id/{video_id}.lock")
        with open(f"by-video-id/{video_id}.dlend", "a") as fp:
            final_timestamp = dt.datetime.utcnow().timestamp()
            res = {'init_timestamp': init_timestamp, 'final_timestamp': final_timestamp, 'exit_cause': exit_cause, 'outfile': outfile}
            fp.write(json.dumps(res))
    finally:
        if fd is not None:
            remove_file_lock(fd)


def write_status(final_status: str, video_id: str, init_timestamp, outfile: str):
    """ Called after the download has finished """
    fd = None
    try:
        fd = create_file_lock(f"by-video-id/{video_id}.lock")
        with open(f"by-video-id/{video_id}.status", "a") as fp:
            final_timestamp = dt.datetime.utcnow().timestamp()
            res = {'init_timestamp': init_timestamp, 'final_timestamp': final_timestamp, 'final_status': final_status, 'outfile': outfile}
            fp.write(json.dumps(res))
    finally:
        if fd is not None:
            remove_file_lock(fd)


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


def run_loop(outname, video_id, init_timestamp):
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

    output_file = f"{outname}.json"
    max_retries = 720   # 12 hours, with 60 second delays

    old_video_id = video_id
    video_id = extract_video_id_from_yturl(video_id, strict=False)
    if old_video_id != video_id:
        print('(downloader) warning: video_id was not a bare id.', file=sys.stderr)

    global cookies_allowed
    cookies_allowed = check_cookies_allowed()

    # Check for cookies at <video_id>.txt
    cookies = try_for_cookies(video_id=video_id, channel_id=None, allow_generic=False)
    if cookies is not None:
        print('(downloader) providing cookies since video-specific cookies present:', cookies)

    # Don't pass cookies if we don't have to.
    downloader = ChatDownloader(cookies=cookies)

    # Forcefully create a YouTube session
    youtube = downloader.create_session(YouTubeChatDownloader)

    try:
        details = youtube.get_video_data(video_id)
        is_live = details.get('status') in {'live', 'upcoming'}
        channel_id = details.get('author_id')
        print('(downloader) initial:', details.get('status'), details.get('video_type'), video_id)
        print('(downloader) title:', details.get('title'))
        print('(downloader) author:', details.get('author'))
        if details.get('title') is None:
            print('title missing, will dump video details')
            print('(downloader) video_id:', video_id)
            print('(downloader)', details)

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
        while True:
            if retry or paranoid_retry:
                if not paranoid_retry:
                    errors += 1

                    if errors == 1:
                        print('(downloader) Waiting 60 seconds before retrying:', video_id)

                    elif errors > max_retries:
                        print('(downloader) Retry limit reached:', video_id)
                        aborted = True

                        break

                # Retry members only video on new cookies immediately.
                if not new_cookies:
                    time.sleep(60)
                else:
                    new_cookies = False

                fd = None
                if not started and private and not new_cookies:
                    # Throttle time-waiting private video tasks to avoid hammering YouTube
                    fd = create_file_lock("private.lock")
                    time.sleep(5)

                try:
                    downloader = ChatDownloader(cookies=cookies)

                    youtube = downloader.create_session(YouTubeChatDownloader)

                    details = youtube.get_video_data(video_id)
                    is_live = details.get('status') in {'live', 'upcoming'}
                    if retry:
                        print('(downloader) retry:', details.get('status'), details.get('video_type'), video_id)
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

            try:
                chat = downloader.get_chat(video_id, output=output_file, message_groups=['all'], indent=2, overwrite=False, interruptible_retry=False)

                private = False

                # chat_downloader feature check
                try:
                    is_live = chat.is_live
                except AttributeError:
                    pass

                if is_live:
                    started = True

                    print('(downloader) Downloading chat from live video:', video_id)
                    if paranoid_retry:
                        print("(downloader) warning: chat downloader exited too soon!", video_id, f"{num_msgs = }", file=sys.stderr)
                        retried = True
                        paranoid_retry = False

                    with open(f"{outname}.stdout", "a") as fp:
                        for message in chat:                        # iterate over messages
                            num_msgs += 1
                            # print the formatted message
                            safe_print(chat.format(message), out=fp)
                            fp.flush()

                    # finished... maybe? we should retry anyway.
                    paranoid_retry = True

                else:
                    if not paranoid_retry:
                        print('(downloader) Video is not live, ignoring:', video_id)
                        missed = True

                    paranoid_retry = False

                    break

            except LoginRequired:
                if not paranoid_retry:
                    print('(downloader) Private video detected:', video_id)
                    private = True

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

                        break

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

            except VideoUnavailable:
                print('(downloader) Removed video detected, giving up:', video_id)

                aborted = True

                break

            except ChatDisabled:
                if not paranoid_retry:
                    print('(downloader) Disabled chat detected:', video_id)
                    # 1 week, 60 second intervals (might be longer if videos are rescheduled)
                    max_retries = max(max_retries, 10080)

                else:
                    break

            except NoChatReplay:
                if not paranoid_retry:
                    print('(downloader) Video is not live, ignoring (no replay):', video_id)
                    missed = True

                break

            except ChatDownloaderError as e:
                print(f'(downloader) {e}:', video_id)
                # 1 day, 60 second intervals
                max_retries = max(max_retries, 1440)

            else:
                retry = False

    except KeyboardInterrupt:
        print('(downloader) warning: got sigint, cancelling download:', video_id, file=sys.stderr)
        aborted = True

    if aborted or errors > max_retries:
        print('(downloader) warning: download incomplete:', video_id, file=sys.stderr)

        if started:
            if retried:
                write_status('started+retried+aborted', video_id, init_timestamp, outname)
            else:
                write_status('started+aborted', video_id, init_timestamp, outname)
        else:
            write_status('aborted', video_id, init_timestamp, outname)

    elif not started and missed:
        print('(downloader) download missed:', video_id)

        if retried:
            write_status('missed+retried', video_id, init_timestamp, outname)
        else:
            write_status('missed', video_id, init_timestamp, outname)

    else:
        print('(downloader) download complete:', video_id)

        if retried:
            write_status('finished+retried', video_id, init_timestamp, outname)
        else:
            write_status('finished', video_id, init_timestamp, outname)

    print('(downloader) download stats:', num_msgs, "messages for video", video_id)

    # Compress logs after the downloader exits.
    if started:
        # Throttle compress tasks to avoid stacking CPU and memory usage
        fd = create_file_lock("compress.lock")
        for ext in ['.json', '.stdout']:
            compress(outname + ext)
        remove_file_lock(fd)


def handle_special_signal(signum, frame):
    # TODO
    pass


def main():
    outname = sys.argv[1]
    video_id = sys.argv[2]
    if len(sys.argv) == 3:
        signal.signal(signal.SIGUSR1, handle_special_signal)
        init_timestamp = dt.datetime.utcnow().timestamp()
        try:
            write_initial_progress('invoked', video_id, init_timestamp, outname)
            run_loop("chat-logs/" + outname, video_id, init_timestamp)
        except Exception:
            write_final_progress('crashed', video_id, init_timestamp, outname)
            print(f"(downloader) fatal exception (pid = {os.getpid()}, ppid = {os.getppid()}, video_id = {video_id}, outname = {outname})")
            traceback.print_exc()
        else:
            write_final_progress('finished', video_id, init_timestamp, outname)

    else:
        print("usage: {} '<outname>' '<video ID>'".format(sys.argv[0]))
        sys.exit(EXIT_BADARG)


if __name__ == '__main__':
    main()
