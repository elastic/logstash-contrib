# encoding: utf-8
module LogStash::Util::Kinesis
  class RecordProcessorFactory

    # Implement the IRecordProcessor interface
    include Java::ComAmazonawsServicesKinesisClientlibraryInterfaces::IRecordProcessorFactory

    def initialize(opts)
      super()
      @opts = opts
    end

    # public IRecordProcessor createProcessor()
    def createProcessor()
      RecordProcessor.new(@opts)
    end
  end
end
