#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "-- fixing bower interaction --"

DATA=$(sed -e '0,/^__DATA__$/d' "$0")
printf '%s\n' "$DATA" > ~/.bowerrc

exit

__DATA__
{
    "interactive": false
}
