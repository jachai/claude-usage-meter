#!/bin/bash
# Floating usage meter. A dedicated Chrome profile is required so app-mode opens a
# real standalone window even when your normal Chrome is already running.
URL="http://127.0.0.1:7654"
PROFILE="$HOME/.claude/usage-watch/meter-window"
curl -s --max-time 3 -o /dev/null "$URL" 2>/dev/null || {
  launchctl kickstart "gui/$(id -u)/com.claude.usage-meter" 2>/dev/null \
    || python3 "$HOME/.claude/scripts/usage_watch.py" serve >/dev/null 2>&1 &
  sleep 2
}
if [ -d "/Applications/Google Chrome.app" ]; then
  mkdir -p "$PROFILE"
  open -na "Google Chrome" --args \
    --app="$URL" --user-data-dir="$PROFILE" \
    --window-size=380,660 --window-position=1480,60 \
    --no-first-run --no-default-browser-check --disable-features=Translate
else
  open "$URL"
fi
