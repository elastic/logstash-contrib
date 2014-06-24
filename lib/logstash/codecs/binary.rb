# encoding: utf-8
require "logstash/codecs/base"

# The 'binary' codec is for passing binary data through logstash with
# no automatic parsing or character set conversion. Raw data will be
# stored in and retrieved from the 'message' field.
#
# This is mainly useful if you want to use logstash as a pipeline for
# shipping around binary data. You might use this codec with inputs like
# RabbitMQ, which sends discrete chunks of binary data: otherwise you'll
# need to split data yourself. With outputs this is more broadly useful
# to just send the content of 'message' to the output with no additional
# formatting.
#
# Most filters will probably not work correctly if the binary data is not
# a UTF-8 string, so you will likely need to write Ruby filters with custom
# code if you want to manipulate the data as it passes through Logstash.
class LogStash::Codecs::Binary < LogStash::Codecs::Base
  config_name "binary"
  milestone 1

  public
  def decode(data)
    yield LogStash::Event.new('message' => data)
  end # def decode

  public
  def encode(event)
    @on_event.call event['message']
  end # def encode

end # class LogStash::Codecs::Binary
