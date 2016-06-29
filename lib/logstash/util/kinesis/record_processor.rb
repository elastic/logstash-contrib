# encoding: utf-8

module LogStash::Util::Kinesis
  class RecordProcessor

    # Implement the IRecordProcessor interface
    include Java::ComAmazonawsServicesKinesisClientlibraryInterfaces::IRecordProcessor

    attr_accessor :logger
    attr_accessor :output_queue

    # Sigh. We have to do some dirty tricks because the interface's initialize
    # method is masked by the ruby constructor.
    # public void initialize(String shardId)
    def initialize(*args)
      if @constructed
        init_processor(args)
      else
        init_object(*args)
      end
    end

    def init_object(opts)
      default_opts = { :checkpoint_interval => 60,
                       :backoff => 3,
                       :retries => 10 }

      opts.merge!(default_opts)
      @opts = opts

      @output_queue = opts[:output_queue]
      @codec = opts[:codec]
      @type = opts[:type]
      @tags = opts[:tags]
      @add_field = opts[:add_field]
      @constructed = true
    end

    def init_processor(shard_id)
      @logger = Cabin::Channel.get(LogStash)
      @shard_id = shard_id
      @logger.info("Initializing record consumer for shard: #{@shard_id}")
      @decoder = Java::JavaNioCharset::Charset.forName("UTF-8").newDecoder()
      @next_checkpoint = Time.now + @opts[:checkpoint_interval]
    end

    #public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer)
    def processRecords(records, checkpointer)
      @logger.info("Processing #{records.size} records from #{@shard_id}");
      records.each do |record|
        begin
          data = @decoder.decode(record.getData).to_s;
          logger.debug("#{record.getSequenceNumber}, #{record.getPartitionKey} #{data}");
          @codec.decode(data) do |event|
            @output_queue << decorate(event)
          end
        rescue
          @logger.error("Failed to log message due to #{$!.to_s}")
        end
      end

        if Time.now > @next_checkpoint
            checkpoint(checkpointer);
            next_checkpoint = Time.now + @opts[:checkpoint_interval]
        end
    end


    #public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason)
    def shutdown(checkpointer, reason)
      @logger.info("Shutting down record processor for shard: #{@shard_id}");
      if (reason == Java::ComAmazonawsServicesKinesisClientlibraryTypes::ShutdownReason::TERMINATE)
            checkpoint(checkpointer);
      end
    end

    # Shamelessly stolen from logstash / lib / logstash / inputs / base.rb
    def decorate(event)
      # Only set 'type' if not already set. This is backwards-compatible behavior
      event["type"] = @type if @type && !event.include?("type")

      if @tags.any?
        event["tags"] ||= []
        event["tags"] += @tags
      end

      @add_field.each do |field, value|
        event[field] = value
      end
      event
    end

    def checkpoint(checkpointer)
      tries = 0
      @logger.info("Issuing checkpoint for #{@shard_id}")
      begin
        checkpointer.checkpoint()
      rescue Java::ComAmazonawsServicesKinesisClientlibraryExceptions::ShutdownException
        @logger.info("Caught shutdown exception, skipping checkpoint.")
      rescue Java::ComAmazonawsServicesKinesisClientlibraryExceptions::ThrottlingException
        if tries >= ( @opts[:retries]-1 )
          @logger.error("Checkpoint failed after #{tries} attempts.")
        else
          @logger.info("Transient issue when checkpointing - attempt #{tries} of #{@opts[:retries]}")
          tries += 1
          sleep(@opts[:backoff])
          retry
        end
      rescue Java::ComAmazonawsServicesKinesisClientlibraryExceptions::InvalidStateException
        @logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.")
      end
    end
  end
end
