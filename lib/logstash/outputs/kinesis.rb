# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"

class LogStash::Outputs::Kinesis < LogStash::Outputs::Base

  config_name "kinesis"
  milestone 1

  include LogStash::PluginMixins::AwsConfig

  config :stream_name, :validate => :string
  config :partition_key_attr, :validate => :string, :default => "@timestamp"
  config :format, :validate => [ "json" ], :default => "json"

  def register

    require "aws-sdk"

    aws_config = aws_options_hash

    @logger.info("Registering kinesis output", :stream_name => @stream_name,
      :partition_key_attr => @partition_key_attr )

    @logger.debug("Kinesis AWS options ", aws_config)

    @kinesis = AWS::Kinesis::Client.new(aws_config);
    @kinesis.describe_stream(:stream_name => @stream_name)
  end

  ## TODO: Detect throttling of put records and backoff accordingly
  def receive(event)
    return unless output?(event)
    pk = event[@partition_key_attr]
    if !pk
        @logger.error("Event missing partition key attribute #{@partition_key_attr}", :event => event)
        return
    else
      pk = pk.to_s
    end
    message = format_event(event)

    if message.bytesize > 50000
      @logger.warn("Kenisis data records are limited to 50kb. Message will be skipped.")
      @logger.debug("Skipped: ", message);
      return
    end

    @logger.debug("Sending to kinesis ", :stream_name => @stream_name,
      :data => message,
      :partition_key => pk);

    @kinesis.put_record(
      :stream_name => @stream_name,
      :data => message,
      :partition_key => pk
      )
  end

  def aws_service_endpoint(region)
    { :kinesis_endpoint => "kinesis.#{region}.amazonaws.com" }
  end

  private
  def format_event(event)
    if (@format == "json")
       message = event.to_json
    else
      raise ArgumentError, "Invalid format #{@format}"
    end
  end

end
