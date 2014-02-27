# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/spool"

# This is the base class for logstash codecs.
class LogStash::Codecs::CloudTrail < LogStash::Codecs::Spool
  config_name "cloudtrail"
  milestone 1

  public
  def decode(data)
    super(JSON.parse(data.force_encoding("UTF-8"))['Records']) do |event|
      event['@timestamp'] = event.delete('eventTime')
      yield LogStash::Event.new(event)
    end
  end # def decode

end # class LogStash::Codecs::CloudTrail
