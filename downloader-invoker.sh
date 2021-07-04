#!/usr/bin/env bash

# Apr 28, 2021

privatevideo_maxretries=1440        # minutes in 1 day
membersonlyvideo_maxretries=1440    # minutes in 1 day
other_maxretries=1440               # minutes in 1 day
chatlessvideo_maxretries=10080      # minutes in 1 week

# Time until next scraper call (NYI)
nextcheck=0

compress_lzip() {
    local filename="${1?}"
    lzip -9 "${filename?}"
}

compress_zstd() {
    local filename="${1?}"
    zstd -f -19 --rm "${filename?}"
}

compress() {
    compress_lzip "$1" || compress_zstd "$1" || echo "(downloader) compression failed" >&2
}

run_chat_downloader() {
    local outname="${1?}";
    local vid="${2?}";
    local pid=$$
    { chat_downloader -o "${outname?}".json --message_groups "all" --indent 2 -- "${vid?}" > "${outname?}".stdout; echo $? > exval."$pid"; } 2>&1 | tee "${outname?}.stderr"
    local exval="$(<"exval.$pid")"
    if [[ "$exval" != 0 ]]; then
        echo "(downloader) chat_downloader exited with exit status $exval" >&2
    fi
    rm "exval.$pid"
}

readonly EXIT_TRUE=0
readonly EXIT_FALSE=1
readonly EXIT_BADARG=1

certify_finished() {
    local vid="${1?}";
    local code="$(./yt-dlp/yt-dlp.sh -q -s --ignore-no-formats-error "${vid?}" --print '%(is_live|0)d%(is_upcoming|0)d')"
    if [[ "$code" == "00" ]]; then
        return $EXIT_TRUE
    else
        echo "(downloader) certify_finished says we aren't finished!" >&2
        return $EXIT_FALSE
    fi
}

run_chat_downloader_waiting() {
    local outname="${1:?}";
    local vid="${2:?}";
    local failures=0
    run_chat_downloader "chat-logs/${outname?}" "${vid?}";
    until test -s "${outname?}.stdout" || certify_finished "${vid?}"; do
        # Likely broken by localization
        if grep -qF 'Private video' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Private video detected" >&2
            ((++failures))
            if ((failures > privatevideo_maxretries)); then
                echo "(downloader) giving up" >&2
                echo "aborted" > "by-video-id/${vid?}"
                exit $EXIT_FALSE
            fi
        elif grep -qF 'Members-only content' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Members only video detected, try again with cookies" >&2
            ((++failures))
            if ((failures > membersonlyvideo_maxretries)); then
                echo "(downloader) giving up" >&2
                echo "aborted" > "by-video-id/${vid?}"
                exit $EXIT_FALSE
            fi
        elif grep -qF 'video has been removed' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Removed video detected, giving up" >&2
            exit $EXIT_FALSE
        elif grep -qF 'Chat is disabled' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Disabled chat detected" >&2
            ((++failures))
            if ((failures > chatlessvideo_maxretries)); then
                echo "(downloader) giving up" >&2
                echo "aborted" > "by-video-id/${vid?}"
                exit $EXIT_FALSE
            fi
        else
            ((++failures))
            if ((failures > other_maxretries)); then
                echo "(downloader) giving up" >&2
                echo "aborted" > "by-video-id/${vid?}"
                exit $EXIT_FALSE
            fi
        fi
        echo '(downloader)' "Retrying chat downloader for video ${vid?}"
        run_chat_downloader "chat-logs/${outname?}" "${vid?}";
        sleep 60;
    done
    echo '(downloader)' "Finished downloading chat for video ${vid?}"
    cd chat-logs
    compress "${outname?}".json
    compress "${outname?}".stdout
    cd ..
    echo '(downloader)' "Finished compressing chat for video ${vid?}"
}

if (( $# == 2 )); then
    run_chat_downloader_waiting "${1:?}" "${2:?}"
else
    echo "usage: $0" '<outname>' '<video ID>'
    exit $EXIT_BADARG
fi
