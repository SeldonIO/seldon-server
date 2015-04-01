#! /usr/bin/env bash
rm -f props.sed
sed -e 's!\([^=]*\)=\(.*\)!s:<\1>:\2:g!' server.properties > props.sed
if [[ "$(uname)" == "Linux" ]]; then
    find . -type f -not -iwholename './.git*' -not -iwholename './props.sed'  -print0 | xargs -0 -I{} sed -f props.sed -i {}
elif [[ "$(uname)" == "Darwin" ]]; then
    find . -type f -not -iwholename './.git*' -not -iwholename './props.sed'  -print0 | xargs -0 sed -f props.sed -i ''
fi
rm -f props.sed

