#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "-- installing vim config --"

DATA=$(sed -e '0,/^__DATA__$/d' "$0")
printf '%s\n' "$DATA" > ~/.vimrc

exit

__DATA__
set nocompatible
set tabstop=4
set autoindent
set shiftwidth=4
set showmode
set incsearch
set ic
set bs=2
set nowrapscan
set ruler
set viminfo=
set expandtab
set vb
set nobackup
set laststatus=2
set noswapfile

if has ("autocmd")
    au!
    au BufRead   ?akefile* set noexpandtab
    au BufEnter  ?akefile* set noexpandtab
    au BufLeave  ?akefile* set expandtab
endif

set nohlsearch
" Switch syntax highlighting on, when the terminal has colors
" Also switch on highlighting the last used search pattern.
if &t_Co > 2 || has("gui_running")
    syntax on
    set hlsearch
endif

if &listchars ==# 'eol:$'
  set listchars=tab:>\ ,trail:-,extends:>,precedes:<,nbsp:+
endif

