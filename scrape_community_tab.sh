#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

if [[ -z "$channelbase" ]]; then
    echo '(channel tab scraper) channel id is empty' >&2
    exit 1
fi

suf_community="/community"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"

# Disable cookied tab scrapes
[[ -f cookies_ok.sh ]] && . cookies_ok.sh || cookies_ok() { return 0; }
if ! cookies_ok; then
    cookies_ok=0
else
    cookies_ok=1
fi

if [[ "$cookies_ok" == 1 ]]; then
    if [[ ! -f "cookies/${channelbase}.cookies" ]] && [[ -f "cookies/${channelbase}.txt" ]]; then
        cp cookies/${channelbase}.{txt,cookies} -av
    fi
    if test -f "cookies/${channelbase}.cookies"; then
        has_cookies=1
        cookie_file="${channelbase}.cookies"
    fi
fi

next_scrape_file="${channelbase}.next_scrape"
curr_time="$(date "+%s")"  # epoch time (seconds)
if [[ -f "${next_scrape_file?}" ]]; then
    saved_next_time="$(<"${next_scrape_file?}")"
    if ((saved_next_time > curr_time)); then
        # throttled
        echo 'throttled...'
        exit 0
    fi
fi

next_time=$((curr_time + (60*5)))  # 5 min throttle time
echo "$next_time" >"$next_scrape_file"

# Note that premieres will only show up here.
url="https://www.youtube.com/channel/$channelbase$suf_community"
if ((has_cookies)); then
    echo 'cookies used!'
    "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community"
else
    echo 'cookies not used!'
    "$ytdlp_cmd" -s -q -j --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community"
fi
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel tab scraper) warning: fetch for ${tmppre}.community exited with error: $ecode" >&2
fi
jq -r <"${tmppre}.community" .url > "${tmppre}.community.url"

# Bail out early if out actions are futile to reduce console spam.
if [[ ! -f "${tmppre}.community.url" ]]; then
    echo "(channel tab scraper) there doesn't seem to be any videos on the community tab, aborting."
    for suffix in .community {.community,.final}.url; do
        rm "${tmppre}${suffix}" 2>/dev/null
    done
    exit 1
fi

# Create an onmilist of urls (with possible duplicates)
touch "${tmppre}.community.url"

mkdir -p channel-cached
touch "channel-cached/${channelbase}.url.all"
# Avoid reading tab results into scraper_oo
touch "channel-cached/${channelbase}.url.tab.all"
sort "channel-cached/${channelbase}.url.all" "channel-cached/${channelbase}.url.tab.all" | uniq > "channel-cached/${channelbase}.url.tab.all.tmp"
mv "channel-cached/${channelbase}.url.tab.all.tmp" "channel-cached/${channelbase}.url.tab.all"


oldcnt="$(wc -l "channel-cached/${channelbase}.url.tab.all" | cut -d ' ' -f 1)"
# 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
if test -s "${tmppre}.community.url"; then
    "$ytdlp_cmd" -s -q -j --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.tab.all" --max-downloads 20 -a - <"${tmppre}.community.url" > "channel-cached/${channelbase}.meta.tab.new"
else
    echo "no urls... cookies_ok=${cookies_ok}"
fi
newcnt="$(wc -l "channel-cached/${channelbase}.url.tab.all" | cut -d ' ' -f 1)"
echo "(channel tab scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.tab.all"
metacnt="$(wc -l "channel-cached/${channelbase}.meta.tab.new" | cut -d ' ' -f 1)"
if ((metacnt > 0)); then
    echo "(channel tab scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.tab.new"

    for id in $(jq -r .id? <"${channelbase?}.meta.tab.new" | grep -vx 'null'); do
        if [[ "${#id}" == 11 ]]; then
            channeldir="by-channel-id/${channelbase}.dir/"
            mkdir -p "${channeldir?}"
            if [[ ! -f "by-channel-id/${channelbase}.dir/${id}" ]]; then
                ts="$(date +"%s.%N")"
                cp -v "cookies/${channelbase}.txt" "./${id}"
                (echo "${ts}" > "${channeldir}/${id}"; ../downloader.py "_${id}_curr-${ts}" "${id}") & jobpid=$!; disown
                echo "$jobpid"
                kill -CONT "$jobpid"
            fi
        fi
    done
fi

for suffix in .community .final.url; do
    rm "${tmppre}${suffix}"
done

# if URL is in url.all but not in meta.new, the scraper will directly scrape the metainfo itself. Else, meta.new is used.


