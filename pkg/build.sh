#!/bin/bash
# We only need to build two packages now, rpm and deb.  Leaving the os/version stuff in case things change.

[ ! -f ../.VERSION.mk ] && make -C .. .VERSION.mk

. ../.VERSION.mk

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

URLSTUB="http://download.elasticsearch.org/logstash/logstash/"

if [ "x$WGET" != "x" ]; then
  DOWNLOAD_COMMAND="wget -q --no-check-certificate -O"
elif [ "x$CURL" != "x" ]; then
    DOWNLOAD_COMMAND="curl -s -L -k -o"
else
  echo "wget or curl are required."
  exit 1
fi

TARGETDIR="$destdir"
SUFFIX=".tar.gz"
FILEPATH="logstash-${VERSION}"
FILENAME=${FILEPATH}${SUFFIX}
TARGET="${destdir}/${FILENAME}"

$DOWNLOAD_COMMAND ${TARGET} ${URLSTUB}${FILENAME}
if [ ! -f "${TARGET}" ]; then
  echo "ERROR: Unable to download ${URLSTUB}${FILENAME}"
  echo "Exiting."
  exit 1
fi

tar -C $destdir -zxf $TARGET
tar -C $destdir -zxf $tar

cd $destdir

PKGFILES=$(find */ -type f | sort -t / -k 2 | tr '/' '\t' | uniq -f 1 -c | tr '\t' '/' | sort -t / -s -k 1n | awk '{print $1, $2}' | grep ^1 | grep logstash-contrib | awk '{print $2}' | sed -e "s#logstash-contrib-.*//##")

cd logstash-contrib-${VERSION}

rsync -R ${PKGFILES} ../$prefix

cd ../../../

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
