#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Download Earthdata files to current directory from list of URLs."
    echo
    echo "Usage: ./download_files.sh /path/to/granule_list"
    echo
fi

if [ ! -f $1 ]; then
    echo "List $1 of granule download links not found."
    exit
fi

if [ ! -f "$HOME/.urs_cookies" ]; then
    echo "Could not find .urs_cookies in home folder, collecting cookies from Earthdata ..."
    touch $HOME/.netrc
    echo "Enter your Earthdata username:"
    read USERNAME
    echo "Enter your Earthdata password:"
    read -r PASSWORD
    echo "machine urs.earthdata.nasa.gov login $USERNAME password $PASSWORD" > $HOME/.netrc
    chmod 0600 $HOME/.netrc

    touch $HOME/.urs_cookies
    wget --load-cookies $HOME/.urs_cookies --save-cookies $HOME/.urs_cookies --keep-session-cookies http://urs.earthdata.nasa.gov
fi

download_file() {
    rm -f "${1}.part"
    if [ ! -f "$1" ]; then
      wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition -nc -O "${1}.part" "$2"
      mv "$1.part" "$1"
    fi
}

export -f download_file

cat $1 | parallel --gnu --colsep ' ' "download_file {1} {2}"
