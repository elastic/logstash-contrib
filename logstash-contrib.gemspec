# -*- encoding: utf-8 -*-
require File.expand_path('../lib/logstash/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jordan Sissel", "Aaron Mildenstein"]
  gem.email         = ["jls@semicomplete.com", "aaron@mildensteins.com"]
  gem.description   = %q{scalable log and event management (search, archive, pipeline)}
  gem.summary       = %q{logstash-contrib - log and event management (contributed bits)}
  gem.homepage      = "http://logstash.net/"
  gem.license       = "Apache License (2.0)"

  gem.files         = `git ls-files`.split($\)
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "logstash-contrib"
  gem.require_paths = ["lib"]
  gem.version       = LOGSTASH_VERSION

  gem.add_runtime_dependency "rack"               #(MIT license)
  gem.add_runtime_dependency "sinatra"            #(MIT license)
  gem.add_runtime_dependency "google-api-client"                #{Apache 2.0 license}
  gem.add_runtime_dependency "heroku"                           #(MIT license)
  gem.add_runtime_dependency "elasticsearch"                    #(Apache 2.0 license)
  gem.add_runtime_dependency "jiralicious", ["0.2.2"]           #(MIT license)
  gem.add_runtime_dependency "mongo"                            #(Apache 2.0 license)
  gem.add_runtime_dependency "onstomp"                          #(Apache 2.0 license)
  gem.add_runtime_dependency "riak-client", ["1.0.3"]           #(Apache 2.0 license)
  gem.add_runtime_dependency "riemann-client", ["0.2.1"]        #(MIT license)
  gem.add_runtime_dependency "uuidtools"                        # For generating amqp queue names (Apache 2.0 license)
  gem.add_runtime_dependency "php-serialize"                    # For input drupal_dblog (MIT license)
  gem.add_runtime_dependency "sequel"                           #(MIT license)
  gem.add_runtime_dependency "jdbc-sqlite3"                     #(MIT license)
  gem.add_runtime_dependency "rsolr"                            #(Apache 2.0 license)
  gem.add_runtime_dependency "jmx4r"                            #(Apache 2.0 license)
  gem.add_runtime_dependency "fog", ["1.20.0"]                  #(MIT license)
  gem.add_runtime_dependency "mac_vendor"                       #(MIT license)

  if RUBY_PLATFORM == 'java'
    gem.platform = RUBY_PLATFORM
    gem.add_runtime_dependency "geoscript", "0.1.0.pre"           #(MIT license)
    gem.add_runtime_dependency "jruby-win32ole"                   #(unknown license)
    gem.add_runtime_dependency "jdbc-mysql"                       # For input drupal_dblog (BSD license)

  else
    gem.add_runtime_dependency "mysql2"   # For input drupal_dblog (MIT license)
  end

end
