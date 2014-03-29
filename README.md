# Logstash Contrib Repository

This is a collection of companion plugins (and hopefully tests, too!) to be
used in conjunction with [Logstash](https://github.com/elasticsearch/logstash).

The plugins here are maintained by the core Logstash team, and supported by the 
community.

## Developing

Logstash will load plugins from logstash-contrib if you use the --pluginpath (-p) 
argument pointing at this project:

    bin/logstash agent -p .../logstash-contrib/lib [options]

See the README.md in the main Logstash project for more information.

## Building

The same build principles which exist in the core Logstash project will apply here.
After installation plugins found herein will be end up in the same path as the core
installation.  Building a tarball package is as simple as:

```
make tarball
```

The resulting package will be found in the `build` directory.

## Project Principles

* Community: If a newbie has a bad time, it's a bug.
* Software: Make it work, then make it right, then make it fast.
* Technology: If it doesn't do a thing today, we can make it do it tomorrow.

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports,
complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and
maintainers or community members  saying "send patches or die" - you will not
see that here.

It is more important to me that you are able to contribute.

For more information about contributing, see the
[CONTRIBUTING](https://github.com/elasticsearch/logstash/blob/master/CONTRIBUTING.md) file.
