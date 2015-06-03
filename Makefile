#
JRUBY_VERSION=1.7.17

WITH_JRUBY=java -jar $(shell pwd)/$(JRUBY) -S
JRUBY=vendor/jar/jruby-complete-$(JRUBY_VERSION).jar
JRUBY_URL=http://jruby.org.s3.amazonaws.com/downloads/$(JRUBY_VERSION)/jruby-complete-$(JRUBY_VERSION).jar
JRUBY_CMD=java -jar $(JRUBY)

PLUGIN_FILES=$(shell git ls-files | egrep '^lib/logstash/(inputs|outputs|filters|codecs)/[^/]+$$' | egrep -v '/(base|threadable).rb$$|/inputs/ganglia/')
QUIET=@
ifeq (@,$(QUIET))
	QUIET_OUTPUT=> /dev/null 2>&1
endif

WGET=$(shell which wget 2>/dev/null)
CURL=$(shell which curl 2>/dev/null)

# OS-specific options
TARCHECK=$(shell tar --help|grep wildcard|wc -l|tr -d ' ')
ifeq (0, $(TARCHECK))
TAR_OPTS=
else
TAR_OPTS=--wildcards
endif

#spec/outputs/graphite.rb spec/outputs/email.rb)
default:
	@echo "Make targets you might be interested in:"
	@echo "  tarball -- builds the tarball"
	@echo "  tarball-test -- runs the test suite against the tarball"

TESTS=$(wildcard spec/inputs/*.rb spec/support/*.rb spec/filters/*.rb spec/examples/*.rb spec/codecs/*.rb spec/conditionals/*.rb spec/event.rb spec/jar.rb)

# The 'version' is generated based on the logstash version, git revision, etc.
.VERSION.mk: REVISION=$(shell git rev-parse --short HEAD | tr -d ' ')
.VERSION.mk: RELEASE=$(shell awk -F\" '/LOGSTASH_VERSION/ {print $$2}' lib/logstash/version.rb | tr -d ' ')
#.VERSION.mk: TAGGED=$(shell git tag --points-at HEAD | egrep '^v[0-9]')
.VERSION.mk: DEV=$(shell echo $RELEASE | egrep '\.dev$$')
.VERSION.mk: MODIFIED=$(shell git diff --shortstat --exit-code > /dev/null ; echo $$?)
.VERSION.mk:
	$(QUIET)echo "RELEASE=${RELEASE}" > $@
	$(QUIET)echo "REVISION=${REVISION}" >> $@
	$(QUIET)echo "DEV=${DEV}" >> $@
	$(QUIET)echo "MODIFIED=${MODIFIED}" >> $@
	$(QUIET)if [ -z "${DEV}" ] ; then \
		if [ "${MODIFIED}" -eq 1 ] ; then \
			echo "VERSION=${RELEASE}-modified" ; \
		else \
			echo "VERSION=${RELEASE}" ; \
		fi ; \
	else \
		if [ "${MODIFIED}" -eq 1 ] ; then \
			echo "VERSION=${RELEASE}-${REVISION}-modified" ; \
		else \
			echo "VERSION=${RELEASE}-${REVISION}" ; \
		fi ; \
	fi >> $@

-include .VERSION.mk

version:
	@echo "Version: $(VERSION)"

# Figure out if we're using wget or curl
.PHONY: wget-or-curl
wget-or-curl:
ifeq ($(CURL),)
ifeq ($(WGET),)
	@echo "wget or curl are required."
	exit 1
else
DOWNLOAD_COMMAND=wget -q --no-check-certificate -O
endif
else
DOWNLOAD_COMMAND=curl -s -L -k -o
endif

.PHONY: clean
clean:
	@echo "=> Cleaning up"
	-$(QUIET)rm -rf .bundle
	-$(QUIET)rm -rf build
	-$(QUIET)rm -f pkg/*.deb
	-$(QUIET)rm .VERSION.mk

.PHONY: clean-vendor
clean-vendor:
	-$(QUIET)rm -rf vendor

.PHONY: copy-ruby-files
copy-ruby-files: | build/ruby
	@# Copy lib/ and test/ files to the root
	$(QUIET)rsync -a --include "*/" --include "*.rb" --exclude "*" ./lib/ ./test/ ./build/ruby
	$(QUIET)rsync -a ./spec ./build/ruby
	$(QUIET)rsync -a ./locales ./build/ruby
	@# Delete any empty directories copied by rsync.
	$(QUIET)find ./build/ruby -type d -empty -delete

vendor:
	$(QUIET)mkdir $@

vendor/jar: | vendor
	$(QUIET)mkdir $@

vendor-jruby: $(JRUBY)

$(JRUBY): | vendor/jar
	$(QUIET)echo " ==> Downloading jruby $(JRUBY_VERSION)"
	$(QUIET)$(DOWNLOAD_COMMAND) $@ $(JRUBY_URL)

# Always run vendor/bundle
.PHONY: fix-bundler
fix-bundler:
	-$(QUIET)rm -rf .bundle

.PHONY: vendor-gems
vendor-gems: | vendor/bundle

.PHONY: vendor/bundle
vendor/bundle: | vendor $(JRUBY)
	@echo "=> Ensuring ruby gems dependencies are in $@..."
	-$(QUIET)$(JRUBY_CMD) gembag.rb logstash-contrib.gemspec
	@# Purge any junk that fattens our jar without need!
	@# The riak gem includes previous gems in the 'pkg' dir. :(
	-$(QUIET)rm -rf $@/jruby/1.9/gems/riak-client-1.0.3/pkg
	@# Purge any rspec or test directories
	-$(QUIET)rm -rf $@/jruby/1.9/gems/*/spec $@/jruby/1.9/gems/*/test
	@# Purge any comments in ruby code.
	@#-find $@/jruby/1.9/gems/ -name '*.rb' | xargs -n1 sed -i -re '/^[ \t]*#/d; /^[ \t]*$$/d'

.PHONY: build
build:
	-$(QUIET)mkdir -p $@

build/ruby: | build
	-$(QUIET)mkdir -p $@

VENDOR_DIR=vendor/bundle/jruby/1.9

build/tarball: | build
	mkdir $@
build/tarball/logstash-%: | build/tarball
	mkdir $@

show:
	echo $(VERSION)

.PHONY: prepare-tarball
prepare-tarball tarball zip: WORKDIR=build/tarball/logstash-contrib-$(VERSION)
prepare-tarball: vendor-gems
prepare-tarball:
	@echo "=> Preparing tarball"
	$(QUIET)$(MAKE) $(WORKDIR)
	$(QUIET)rsync -a --relative lib spec vendor/bundle/jruby --exclude 'vendor/bundle/jruby/1.9/cache' --exclude 'vendor/bundle/jruby/1.9/gems/*/doc' --exclude 'lib/logstash/version.rb'  $(WORKDIR)
#	$(QUIET)sed -i -e 's/^LOGSTASH_VERSION = .*/LOGSTASH_VERSION = "$(VERSION)"/' $(WORKDIR)/lib/logstash/version.rb

.PHONY: tarball
tarball: | build/logstash-contrib-$(VERSION).tar.gz
build/logstash-contrib-$(VERSION).tar.gz: | prepare-tarball
	$(QUIET)tar -C $$(dirname $(WORKDIR)) -c $$(basename $(WORKDIR)) \
		| gzip -9c > $@
	@echo "=> tarball ready: $@"

.PHONY: zip
zip: | build/logstash-contrib-$(VERSION).zip
build/logstash-contrib-$(VERSION).zip: | prepare-tarball
	$(QUIET)(cd $$(dirname $(WORKDIR)); find $$(basename $(WORKDIR)) | zip $(PWD)/$@ -@ -9)$(QUIET_OUTPUT)
	@echo "=> zip ready: $@"

.PHONY: tarball-test
tarball-test: #build/logstash-contrib-$(VERSION).tar.gz
	$(QUIET)-rm -rf build/test-tarball/
	$(QUIET)mkdir -p build/test-tarball/
	tar -C build/test-tarball --strip-components 1 -xf build/logstash-contrib-$(VERSION).tar.gz
	(cd build/test-tarball; bin/logstash rspec $(TESTS) --fail-fast)

package: | tarball
	sh pkg/build2.sh centos 6
	sh pkg/build2.sh ubuntu 12.04
