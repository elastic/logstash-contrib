# NOTICE: Please read!

## This repository is deprecated!

This repository and its contents are specific to the 1.4 branch of Logstash.  **Beginning with Logstash 1.5, a new plugin management system was put into place.**  No longer will all plugins be shipped with the Logstash core, they will be completely independent from one another.

This means that **new pull requests will not be accepted** here.  This also means that any **existing pull requests will likely need to be rerouted to their new repository**.  You can find where the new plugin repository is below.

## [github.com/logstash-plugins](http://github.com/logstash-plugins)

Repositories are now in [github.com/logstash-plugins](http://github.com/logstash-plugins) and have a `logstash-{type}-{name}` syntax, where type is one of `input`, `codec`, `filter`, or `output`.  For example, the Elasticsearch output plugin is at [github.com/logstash-plugins/logstash-output-elasticsearch](http://github.com/logstash-plugins/logstash-output-elasticsearch), and the collectd codec is at [github.com/logstash-plugins/logstash-codec-collectd](http://github.com/logstash-plugins/logstash-codec-collectd).


