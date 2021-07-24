#!/usr/bin/env python3
import sys
import time
import subprocess
import signal

from chat_downloader import ChatDownloader
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

# testing
ids = [
    'p1KolyCqICI',  # past broadcast
    'XWq5kBlakcQ',  # chat disabled
    'vprErlL1w2E',  # members only
    'xxxxxxxxxxx',  # removed/does not exist
    '5qap5aO4i9A',  # live video
]

downloader = ChatDownloader()  # modify this if cookies are necessary

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


def run_loop(outname, video_id):
    """ Download chat, retrying on errors """
    errors = 0
    retry = False
    paranoid_retry = False
    aborted = False

    output_file = f"{outname}.json"
    max_retries = 720   # 12 hours, with 60 second delays

    try:
        while True:
            if retry:
                errors += 1

                if errors == 1:
                    print('(downloader) Waiting 60 seconds before retrying:', video_id)

                elif errors > max_retries:
                    print('(downloader) Retry limit reached:', video_id)
                    aborted = True

                    break

                time.sleep(60)

            retry = True

            try:
                chat = downloader.get_chat(video_id, output=output_file, message_groups=['all'], indent=2, overwrite=False)

                if chat.is_live:
                    print('(downloader) Downloading chat from live video:', video_id)
                    if paranoid_retry:
                        print("(downloader) warning: chat downloader exited too soon!", file=sys.stderr)
                        paranoid_retry = False

                    with open(f"{outname}.stdout", "a") as fp:
                        for message in chat:                        # iterate over messages
                            # print the formatted message
                            safe_print(chat.format(message), out=fp)

                    # finished... maybe? we should retry anyway.
                    paranoid_retry = True

                else:
                    if not paranoid_retry:
                        print('(downloader) Video is not live, ignoring:', video_id)

                    paranoid_retry = False

                    break

            except LoginRequired:
                if not paranoid_retry:
                    print('(downloader) Private video detected:', video_id)

                else:
                    break

            except VideoUnplayable:
                print('(downloader) Members only video detected, try again with cookies:', video_id)

            except VideoUnavailable:
                print('(downloader) Removed video detected, giving up:', video_id)

                aborted = True

                break

            except (ChatDisabled, NoChatReplay):
                if not paranoid_retry:
                    print('(downloader) Disabled chat detected:', video_id)
                    # 1 week, 60 second intervals (might be longer if videos are rescheduled)
                    max_retries = max(max_retries, 10080)

                else:
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
    else:
        print('(downloader) download complete:', video_id)

    # Compress logs after the downloader exits.
    for ext in ['.json', '.stdout']:
        compress(outname + ext)


def handle_special_signal(signum, frame):
    # TODO
    pass


if __name__ == '__main__':
    if len(sys.argv) == 3:
        signal.signal(signal.SIGUSR1, handle_special_signal)
        run_loop("chat-logs/" + sys.argv[1], sys.argv[2])

    else:
        print("usage: {} '<outname>' '<video ID>'".format(sys.argv[0]))
        sys.exit(EXIT_BADARG)
