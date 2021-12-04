#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

if [[ -z "$channelbase" ]]; then
    echo '(channel membership tab scraper) channel id is empty' >&2
    exit 1
fi

suf_membership="/membership"
suf_community="/community"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"

# Disable cookied tab scrapes
[[ -f cookies_ok.sh ]] && . cookies_ok.sh || cookies_ok() { return 0; }
if ! cookies_ok; then
    cookies_ok=0
    echo "cookies not safe to use, how are we going to get membership videos now?" >&2
    exit 1
else
    cookies_ok=1
fi

if [[ "$cookies_ok" == 1 ]]; then
    if [[ ! -f "cookies/${channelbase}.cookies" ]] && [[ -f "cookies/${channelbase}.txt" ]]; then
        cp cookies/${channelbase}.{txt,cookies} -av
    fi
    if test -f "cookies/${channelbase}.cookies"; then
        has_cookies=1
        cookie_file="cookies/${channelbase}.cookies"
    else
        echo "cookies not found, how are we going to get membership videos now?" >&2
        exit 1
    fi
fi

next_scrape_file="${channelbase}.mem.next_scrape"
curr_time="$(date "+%s")"  # epoch time (seconds)
if [[ -f "${next_scrape_file?}" ]]; then
    saved_next_time="$(<"${next_scrape_file?}")"
    if ((saved_next_time > curr_time)); then
        # throttled
        echo "throttled... (remaining: $((saved_next_time - curr_time))s)"
        exit 0
    fi
fi

next_time=$((curr_time + (60*15)))  # 15 min throttle time
echo "$next_time" >"$next_scrape_file"

# Note that if cookies are bad, a redirection may occur.
url="https://www.youtube.com/channel/$channelbase$suf_membership"
if ((has_cookies)); then
    "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" | grep -vF '/channel/' >"${tmppre}.membership"
else
    echo '(channel membership tab scraper) cookies not used!' >&2
    false
fi
ecode=$?
curr_time="$(date "+%s")"  # epoch time (seconds)
if [[ "$ecode" != 0 ]]; then
    echo "(channel membership tab scraper) warning: fetch for ${tmppre} (membership tab). exited with error: $ecode" >&2
    echo "$curr_time" > "${channelbase}.mem.last_failure"
else
    echo "$curr_time" > "${channelbase}.mem.last_success"
fi
jq -r <"${tmppre}.membership" 'select(.id != null)|.id' > "${tmppre}.membership.url"

# If membership returned no results (A/B test?), try community page.
url="https://www.youtube.com/channel/$channelbase$suf_community"
if [[ ! -s "${tmppre}.membership.url" ]]; then
    if ((has_cookies)); then
        echo 'no membership videos on membership tab, trying community tab'
        "$ytdlp_cmd" -s -q -j --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community.nocookies"
        ecode=$?
        curr_time="$(date "+%s")"  # epoch time (seconds)
        if [[ "$ecode" != 0 ]]; then
            echo "(channel membership tab scraper) warning: fetch for ${tmppre} (community tab, no cookies). exited with error: $ecode" >&2
            echo "$curr_time" > "${channelbase}.comm.last_failure"
        else
            echo "$curr_time" > "${channelbase}.comm.last_success"
        fi
        jq -r <"${tmppre}.community.nocookies" 'select(.id != null)|.id' | sort | uniq > "${tmppre}.community.nocookies.url"

        "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community.withcookies"
        ecode=$?
        curr_time="$(date "+%s")"  # epoch time (seconds)
        if [[ "$ecode" != 0 ]]; then
            echo "(channel membership tab scraper) warning: fetch for ${tmppre} (community tab, with cookies). exited with error: $ecode" >&2
            echo "$curr_time" > "${channelbase}.comm-cookied.last_failure"
        else
            echo "$curr_time" > "${channelbase}.comm-cookied.last_success"
        fi
        jq -r <"${tmppre}.community.withcookies" 'select(.id != null)|.id' | sort | uniq > "${tmppre}.community.withcookies.url"

        comm -13 <(sort "${tmppre}.community.nocookies.url" | uniq) <(sort "${tmppre}.community.withcookies.url" | uniq) | grep -vF '/channel/' >"${tmppre}.membership"
    fi
    cp "${tmppre}.membership" "${tmppre}.membership.url"
    foundcnt_known="$(wc -l "${tmppre}.membership.url" | cut -d ' ' -f 1)"
    foundcnt_no="$(wc -l "${tmppre}.community.nocookies.url" | cut -d ' ' -f 1)"
    foundcnt_with="$(wc -l "${tmppre}.community.withcookies.url" | cut -d ' ' -f 1)"
    echo "(channel membership tab scraper)" "${foundcnt_known?} member video entries ("${foundcnt_no?}" public videos on community tab, ${foundcnt_with?} with cookies)"
fi


# FIXME: jq always makes this file.
# Bail out early if out actions are futile to reduce console spam.
if [[ ! -f "${tmppre}.membership.url" ]]; then
    echo "(channel membership tab scraper) there doesn't seem to be any videos on the membership tab, aborting."
    for suffix in .membership .membership.url; do
        rm "${tmppre}${suffix}" 2>/dev/null
    done
    exit 1
fi

# Create an onmilist of urls (with possible duplicates)
touch "${tmppre}.membership.url"

mkdir -p channel-cached
# Avoid reading membership tab results into scraper_oo
touch "channel-cached/${channelbase}.url.mem.all"
sort "channel-cached/${channelbase}.url.mem.all" | uniq > "channel-cached/${channelbase}.url.mem.all.tmp"
mv "channel-cached/${channelbase}.url.mem.all.tmp" "channel-cached/${channelbase}.url.mem.all"


oldcnt="$(wc -l "channel-cached/${channelbase}.url.mem.all" | cut -d ' ' -f 1)"
# 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
if test -s "${tmppre}.membership.url"; then
    "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.mem.all" --max-downloads 20 -a - < <(grep -vE '/channel/' "${tmppre}.membership.url") > "channel-cached/${channelbase}.meta.mem.new"
else
    echo "(channel membership tab scraper) no urls... cookies_ok=${cookies_ok}" >&2
fi
newcnt="$(wc -l "channel-cached/${channelbase}.url.mem.all" | cut -d ' ' -f 1)"
echo "(channel membership tab scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.mem.all"
metacnt="$(wc -l "channel-cached/${channelbase}.meta.mem.new" | cut -d ' ' -f 1)"
if ((metacnt > 0)); then
    echo "(channel membership tab scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.mem.new"
fi

for suffix in .membership .membership.url; do
    rm "${tmppre}${suffix}"
done
