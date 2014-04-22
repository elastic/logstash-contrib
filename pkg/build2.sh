#!/bin/bash

basedir=$(dirname $0)/../

# Get version details
[ ! -f "$basedir/.VERSION.mk" ] && make -C $basedir .VERSION.mk
. $basedir/.VERSION.mk

if [ "$#" -ne 2 ] ; then
  echo "Usage: $0 os release"
  echo
  echo "Example: $0 ubuntu 12.10"
fi

silent() {
  "$@" > /dev/null 2>&1
}
log() {
  echo "$@" >&2
}

os=$1
osver=$2

logstash="$basedir/../logstash/"
logstashtar="$basedir/../logstash/build/logstash-${VERSION}.tar.gz"
contribtar="$basedir/build/logstash-contrib-${VERSION}.tar.gz"

workdir=build/$os/$osver
silent mkdir -p $workdir 

if [ ! -d "$logstash" ] ; then
  echo "Missing logstash git repo? Expected to find it here: $logstash"
  exit 1
fi

if [ ! -f "$logstashtar" ] ; then
  echo "Missing $logstashtar"
  exit 1
fi

if [ ! -f "$contribtar" ] ; then
  echo "Building $contribtar"
  make -C $basedir $contribtar || exit 1
fi

# Find files that only contrib contains.
tarfiles() {
  log "Listing files in $1" 
  tar -ztf "$1" | sed -re 's@^[^/]+/@@' | sort
}
tarfiles $contribtar > $workdir/files.contrib
tarfiles $logstashtar > $workdir/files.logstash

# Find all files in contrib but not logstash core
grep -Fvxf $workdir/files.logstash $workdir/files.contrib > $workdir/files
set -x
set -e

# Unpack the contrib tarball.
silent mkdir -p $workdir/tarball  || true
tar -C $workdir/tarball -zxf $contribtar --strip-components 1

_fpm() {
  target=$1
  fpm -s dir -n logstash-contrib -v "$RELEASE" \
    -a noarch --url "https://github.com/elasticsearch/logstash-contrib" \
    --description "Community supported plugins for Logstash" \
    -d "logstash = $RELEASE" \
    --vendor "Elasticsearch" \
    --license "Apache 2.0" \
    "$@"
}
case $os in
  centos|fedora|redhat|sl)
    _fpm -t rpm --rpm-use-file-permissions --rpm-user root --rpm-group root \
      --iteration "1_$REVISION" --rpm-ignore-iteration-in-dependencies \
      -f -C $workdir/tarball --prefix /opt/logstash $(cat $workdir/files | head -1)
    ;;
  ubuntu|debian)
    _fpm -t deb --deb-user root --deb-group root \
      --iteration "1-$REVISION" --deb-ignore-iteration-in-dependencies \
      -f -C $workdir/tarball --prefix /opt/logstash $(cat $workdir/files | head -1)
    ;;
esac
