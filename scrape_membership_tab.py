#!/usr/bin/env python3
import sys
import os
import re
import subprocess
import shutil
from utils import get_timestamp_now

try:
    from cookies_ok import are_cookies_ok
except ImportError:
    def are_cookies_ok():
        # Assume we are scraping from an IP we're fine with
        return True


def get_channelbase(channelbase):
    """ Channel URL to ID """
    channelregex = re.compile("/channel/([^/]+)")
    res = channelregex.findall(channelbase)
    if len(res):
        return res[0]
    else:
        if channelbase.startswith('UC'):
            return channelbase
        else:
            print('(channel membership tab scraper) channel id is empty or invalid', file=sys.stderr)
            sys.exit(1)


channelbase = get_channelbase(sys.argv[1])

suf_membership = "/membership"
suf_community = "/community"

tmppre = f"tmp.{channelbase}"
try:
    os.remove(f"{tmppre}.url")
except OSError:
    pass

ytdlp_cmd = "../yt-dlp/yt-dlp.sh"

# Disable cookied tab scrapes
if not are_cookies_ok():
    print("(channel membership tab scraper) cookies not safe to use, how are we going to get membership videos now?", file=sys.stderr)
    sys.exit(1)


def install_cookies():
    if os.path.exists(f'cookies/{channelbase}.txt'):
        shutil.copy2(f'cookies/{channelbase}.txt', f'cookies/{channelbase}.cookies')

    if not os.path.exists(f"cookies/{channelbase}.cookies"):
        print("(channel membership tab scraper) cookies not found, how are we going to get membership videos now?", file=sys.stderr)
        sys.exit(1)


cookie_file = f"cookies/{channelbase}.cookies"

next_scrape_file = f"{channelbase}.mem.next_scrape"
curr_time = int(get_timestamp_now())  # epoch time (seconds)
if os.path.exists(f'{next_scrape_file}'):
    saved_next_time = int(open(next_scrape_file).read().strip())
    if saved_next_time > curr_time:
        # throttled
        print(f"(channel membership tab scraper) throttled... (remaining: {(saved_next_time - curr_time)}s)")
        sys.exit(0)


def write_time_to_file(file, ts):
    with open(f'{file}', 'w') as fp:
        fp.write(str(ts))


def file_empty(path):
    return os.stat(path).st_size == 0


def file_linecount(path):
    with open(path) as fp:
        return len(fp.readlines())


def file_touch(path):
    with open(path, 'a+'):
        # touched
        pass


next_time = int(get_timestamp_now()) + (60 * 3)  # 3 min throttle time
write_time_to_file(f'{next_scrape_file}', next_time)

# Note that if cookies are bad, a redirection may occur.
url = f"https://www.youtube.com/channel/{channelbase}{suf_membership}"

# Get raw data on membership videos from membership tab
proc = subprocess.run(f'"{ytdlp_cmd}" -s -q -j --cookies="{cookie_file}" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "{url}" | grep -vF /channel/ >"{tmppre}.membership"', shell=True)
curr_time = int(get_timestamp_now())  # epoch time (seconds)
if proc.returncode != 0:
    print(f"(channel membership tab scraper) warning: fetch for {tmppre} (membership tab) exited with error: {proc.returncode}", file=sys.stderr)
    write_time_to_file(f'{channelbase}.mem.last_failure', curr_time)
else:
    write_time_to_file(f'{channelbase}.mem.last_success', curr_time)

# Extract membership video URLs
proc = subprocess.run(f'jq -r <"{tmppre}.membership" \'select(.id != null)|.id\' > "{tmppre}.membership.url"', shell=True)
if proc.returncode != 0:
    print(f"(channel membership tab scraper) error: processing {tmppre}.membership (membership tab) exited with error: {proc.returncode}", file=sys.stderr)

# If membership returned no results (A/B test?), try community page.
url = f"https://www.youtube.com/channel/{channelbase}{suf_community}"
if file_empty(f"{tmppre}.membership.url"):
    print('(channel membership tab scraper) no membership videos on membership tab, trying community tab')

    # Get raw data on videos from community tab (no cookies sent)
    proc = subprocess.run(f'"{ytdlp_cmd}" -s -q -j --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "{url}" >"{tmppre}.community.nocookies"', shell=True)
    curr_time = int(get_timestamp_now())  # epoch time (seconds)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) warning: fetch for {tmppre} (community tab, no cookies) exited with error: {proc.returncode}", file=sys.stderr)
        write_time_to_file(f'{channelbase}.comm.last_failure', curr_time)
    else:
        write_time_to_file(f'{channelbase}.comm.last_success', curr_time)
    # Extract community tab video ids (no cookies sent)
    proc = subprocess.run(f'jq -r <"{tmppre}.community.nocookies" \'select(.id != null)|.id\' | sort | uniq > "{tmppre}.community.nocookies.url"', shell=True)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) error: processing {tmppre}.community.nocookies (community tab) exited with error: {proc.returncode}", file=sys.stderr)

    # Get raw data on videos from community tab (cookies sent)
    proc = subprocess.run(f'"{ytdlp_cmd}" -s -q -j --cookies="{cookie_file}" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "{url}" >"{tmppre}.community.withcookies"', shell=True)
    curr_time = int(get_timestamp_now())  # epoch time (seconds)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) warning: fetch for {tmppre} (community tab, with cookies) exited with error: {proc.returncode}", file=sys.stderr)
        write_time_to_file(f'{channelbase}.comm-cookied.last_failure', curr_time)
    else:
        write_time_to_file(f'{channelbase}.comm-cookied.last_success', curr_time)
    # Extract community tab video ids (cookies sent)
    proc = subprocess.run(f'jq -r <"{tmppre}.community.withcookies" \'select(.id != null)|.id\' | sort | uniq > "{tmppre}.community.withcookies.url"', shell=True)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) error: processing {tmppre}.community.withcookies (community tab) exited with error: {proc.returncode}", file=sys.stderr)

    # Separate member-only videos
    proc = subprocess.run(f'comm -13 <(sort "{tmppre}.community.nocookies.url" | uniq) <(sort "{tmppre}.community.withcookies.url" | uniq) | grep -vF /channel/ >"{tmppre}.membership"', shell=True)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) error: member video separation failed with error: {proc.returncode}", file=sys.stderr)
        sys.exit(1)
    shutil.copy2(f'{tmppre}.membership', f'{tmppre}.membership.url')

    # Report results
    foundcnt_known = file_linecount(f'{tmppre}.membership.url')
    foundcnt_no = file_linecount(f'{tmppre}.community.nocookies.url')
    foundcnt_with = file_linecount(f'{tmppre}.community.withcookies.url')
    print(f'(channel membership tab scraper) {foundcnt_known} member video entries ({foundcnt_no} public videos on community tab, {foundcnt_with} with cookies)')

if file_empty(f"{tmppre}.membership.url"):
    print("(channel membership tab scraper) we seem unable to find any members-only videos, aborting")
    sys.exit(1)

# Phase 2, meta collection

# Create an onmilist of urls (with possible duplicates)

os.makedirs('channel-cached', exist_ok=True)
# Avoid reading membership tab results into scraper_oo
file_touch(f"channel-cached/{channelbase}.url.mem.all")
proc = subprocess.run(f'sort "channel-cached/{channelbase}.url.mem.all" | uniq > "channel-cached/{channelbase}.url.mem.all.tmp"', shell=True)
if proc.returncode != 0:
    print(f"(channel membership tab scraper) error: sort|uniq failed with error: {proc.returncode}", file=sys.stderr)
    sys.exit(1)
shutil.move(f"channel-cached/{channelbase}.url.mem.all.tmp", f"channel-cached/{channelbase}.url.mem.all")


oldcnt = file_linecount(f'channel-cached/{channelbase}.url.mem.all')
# 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
if not file_empty(f"{tmppre}.membership.url"):
    proc = subprocess.run(f'"{ytdlp_cmd}" -s -q -j --cookies="{cookie_file}" --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/{channelbase}.url.mem.all" --max-downloads 20 -a - < <(grep -vE /channel/ "{tmppre}.membership.url") > "channel-cached/{channelbase}.meta.mem.new"', shell=True)
    if proc.returncode != 0:
        print(f"(channel membership tab scraper) error: meta fetch with download archive failed with error: {proc.returncode}", file=sys.stderr)
        sys.exit(1)
else:
    print("(channel membership tab scraper) no urls...", file=sys.stderr)
newcnt = file_linecount(f'channel-cached/{channelbase}.url.mem.all')
print(f"(channel membership tab scraper) {newcnt} (+{(newcnt - oldcnt)}) entries now in channel-cached/{channelbase}.url.mem.all")
metacnt = file_linecount(f'channel-cached/{channelbase}.meta.mem.new')
if metacnt > 0:
    print(f"(channel membership tab scraper) {metacnt} entries now in channel-cached/{channelbase}.meta.mem.new")

for suffix in ['.membership', '.community.nocookies', '.community.withcookies']:
    try:
        os.remove(f"{tmppre}{suffix}")
    except OSError:
        pass
