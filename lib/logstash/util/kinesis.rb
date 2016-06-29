# encoding: utf-8
require 'logstash/environment'
require 'logstash/logging'

Dir[File.join(LogStash::Environment::JAR_DIR,'aws','*.jar')].each do |file|
  require file
end

LogStash::Logger.setup_log4j(Cabin::Channel.get(LogStash))

require 'logstash/util/aws_basic_credential_provider'
require 'logstash/util/kinesis/record_processor'
require 'logstash/util/kinesis/record_processor_factory'
