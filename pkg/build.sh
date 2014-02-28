#!/bin/bash
# We only need to build two packages now, rpm and deb.  Leaving the os/version stuff in case things change.

basedir=$(pwd)
tmpdir=$basedir/tmp
mkdir -p $tmpdir

[ ! -f $basedir/../.VERSION.mk ] && make -C .. .VERSION.mk

. $basedir/../.VERSION.mk

if ! git show-ref --tags | grep -q "$(git rev-parse HEAD)"; then
	# HEAD is not tagged, add the date, time and commit hash to the revision
	BUILD_TIME="$(date +%Y%m%d%H%M)"
	DEB_REVISION="${BUILD_TIME}~${REVISION}"
	RPM_REVISION=".${BUILD_TIME}.${REVISION}"
fi

URL="http://github.com/elasticsearch/logstash-contrib"
DESCRIPTION="Community contributed plugins for Logstash"

if [ "$#" -ne 2 ] ; then
  echo "Usage: $0 <os> <release>"
  echo 
  echo "Example: $0 ubuntu 12.10"
  exit 1
fi

os=$1
release=$2

echo "Building package for $os $release"

destdir=build/$(echo "$os" | tr ' ' '_')
prefix=/opt/logstash

if [ "$destdir/$prefix" != "/" -a -d "$destdir/$prefix" ] ; then
  rm -rf "$destdir/$prefix"
fi

mkdir -p $destdir/$prefix

# Deploy the tarball to /opt/logstash
tar="$(dirname $0)/../build/logstash-contrib-$VERSION.tar.gz"
if [ ! -f "$tar" ] ; then
echo "Unable to find $tar"
exit 1
fi

WGET=$(which wget 2>/dev/null)
CURL=$(which curl 2>/dev/null)

[ -z "$URLSTUB" ] && URLSTUB="http://download.elasticsearch.org/logstash/logstash/"

if [ "x$WGET" != "x" ]; then
  DOWNLOAD_COMMAND="wget -q -c -O"
elif [ "x$CURL" != "x" ]; then
    DOWNLOAD_COMMAND="curl -s -C -L -o"
else
  echo "wget or curl are required."
  exit 1
fi

TARGETDIR="$tmpdir"
SUFFIX=".tar.gz"
FILEPATH="logstash-${VERSION}"
FILENAME=${FILEPATH}${SUFFIX}
TARGET="${tmpdir}/${FILENAME}"

$DOWNLOAD_COMMAND ${TARGET} ${URLSTUB}${FILENAME}
if [ ! -f "${TARGET}" ]; then
  echo "ERROR: Unable to download ${URLSTUB}${FILENAME}"
  echo "Exiting."
  exit 1
fi

tar -C $tmpdir -zxf $TARGET
tar -C $tmpdir -zxf $tar

cd $tmpdir

###
### Epic one-liner used to find files that exist in contrib and NOT have a match 
### in core.  
### This finds all files in both extracted tarballs and counts file occurrences,
### then finds the ones that have a count of 1 in the logstash-contrib-$VERSION 
### directory (meaning that they don't exist in the logstash-$VERSION directory.  
### It puts these in $PKGFILES and we use that to make the packages
###
### Per pipe breakdown of commands:
#
# find */ -type f       |            # traverse all the directories
# sort -t / -k 2        |            # sort, ignoring the first field
# tr '/' '\t'           |            # turn / into tabs
# uniq -f 1 -c          |            # count duplicates, ignoring the first field
# tr '\t' '/'           |            # turn tabs back into /
# sort -t / -s -k 1n    |            # sort by the number of occurrences
# awk '{print $1, $2}'  |            # remove leading spaces but still print both columns
# grep ^1               |            # get lines starting with 1
# grep logstash-contrib |            # get lines containing logstash-contrib
# awk '{print $2}'      |            # Don't need the count now, so prune that column
# sed -e "s#logstash-contrib-.*//##" # Prune leading directory name

PKGFILES=$(find */ -type f | sort -t / -k 2 | tr '/' '\t' | uniq -f 1 -c | tr '\t' '/' | sort -t / -s -k 1n | awk '{print $1, $2}' | grep ^1 | grep logstash-contrib | awk '{print $2}' | sed -e "s#logstash-contrib-${VERSION}/##" -e "s#^/##" )

cd logstash-contrib-${VERSION}

rsync -R ${PKGFILES} $basedir/$destdir/$prefix

cd $basedir

case $os in
  centos|fedora|redhat|sl) 
    fpm -s dir -t rpm -n logstash-contrib -v "$RELEASE" \
      -a noarch --iteration "1_${RPM_REVISION}" \
      --url "$URL" \
      --description "$DESCRIPTION" \
      -d "logstash = $RELEASE" \
      --vendor "Elasticsearch" \
      --license "Apache 2.0" \
      --rpm-use-file-permissions \
      --rpm-user root --rpm-group root \
      -f -C $destdir .
    ;;
  ubuntu|debian)
    if ! echo $RELEASE | grep -q '\.(dev\|rc.*)'; then
      # This is a dev or RC version... So change the upstream version
      # example: 1.2.2.dev => 1.2.2~dev
      # This ensures a clean upgrade path.
      RELEASE="$(echo $RELEASE | sed 's/\.\(dev\|rc.*\)/~\1/')"
    fi

    fpm -s dir -t deb -n logstash-contrib -v "$RELEASE" \
      -a all --iteration "1-${DEB_REVISION}" \
      --url "$URL" \
      --description "$DESCRIPTION" \
      --vendor "Elasticsearch" \
      --license "Apache 2.0" \
      -d "logstash(= $VERSION)" \
      --deb-user root --deb-group root \
      -f -C $destdir .
    ;;
esac
