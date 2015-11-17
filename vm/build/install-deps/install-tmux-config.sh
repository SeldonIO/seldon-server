#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "-- installing tmux config --"

DATA=$(sed -e '0,/^__DATA__$/d' "$0")
printf '%s\n' "$DATA" > ~/.tmux.conf

exit

__DATA__
set -g mouse-select-pane on

