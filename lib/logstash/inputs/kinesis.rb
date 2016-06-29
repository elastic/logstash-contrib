# encoding: utf-8
require "logstash/util/kinesis"
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"

class LogStash::Inputs::Kinesis < LogStash::Inputs::Base

  config_name "kinesis"
  milestone 1

  include LogStash::PluginMixins::AwsConfig

  default :codec, "json"

  config :stream_name, :validate => :string
  config :application_name, :validate => :string, :default => 'LogStash'

  def register

    require "aws-sdk"

    aws_config = aws_options_hash

    # Java signature
    # KinesisClientLibConfiguration(String applicationName,
    #        String streamName,
    #        AWSCredentialsProvider kinesisCredentialsProvider,
    #        AWSCredentialsProvider dynamoDBCredentialsProvider,
    #        AWSCredentialsProvider cloudWatchCredentialsProvider,
    #        String workerId)
    @credential_provider = LogStash::Util::AwsBasicCredentialProvider.new(
      aws_config[:access_key_id],
      aws_config[:secret_access_key])

    @worker_id = Java::JavaNet::InetAddress.getLocalHost().getCanonicalHostName() + ":" + Java::JavaUtil::UUID.randomUUID().to_s;

    @worker_id = Java::JavaNet::InetAddress.getLocalHost().getCanonicalHostName()

    @kinesis_config = Java::ComAmazonawsServicesKinesisClientlibraryLibWorker::KinesisClientLibConfiguration.new(
      @application_name,
      @stream_name,
      @credential_provider,
      @worker_id
    )

    @logger.info("Registering kinesis input stream", :stream_name => @stream_name)

  end

  def run(output_queue)
    factory = LogStash::Util::Kinesis::RecordProcessorFactory.new(
      :output_queue => output_queue,
      :codec => @codec,
      :type => @type,
      :tags => @tags,
      :add_field => @add_field )

    @worker = Java::ComAmazonawsServicesKinesisClientlibraryLibWorker::Worker.new(factory.to_java, @kinesis_config.to_java)
    begin
      @worker.run
    rescue LogStash::ShutdownSignal
      # Do nothing, shutdown.
    ensure
      teardown
    end
  end

  def teardown()
    @logger.error("Shutting down")
    @worker.shutdown
    finished
  end
  def aws_service_endpoint(region)
    { :kinesis_endpoint => "kinesis.#{region}.amazonaws.com" }
  end

end
