require 'test_utils'
require 'logstash/outputs/simple_kafka'
require 'mocha/api'
require 'poseidon'

describe LogStash::Outputs::SimpleKafka do
  extend LogStash::RSpec

  config <<-CONFIG
    input {
      generator {
        message => "foo-bar"
        count   => 1
        type    => "generator"
      }
    }

    output {
      simple_kafka {
        host      => "localhost"
        port      => 9093
        topic_id  => "logstash.event"
        client_id => "logstash-output"
      }
    }
  CONFIG

  let(:producer) { double(Poseidon::Producer) }

  before do
    Poseidon::Producer.should_receive(:new).with(["localhost:9093"], "logstash-output").and_return(producer)
    producer.should_receive(:send_messages).once.with do |messages|
      msg = messages.first

      expect(msg).to be_kind_of(Poseidon::MessageToSend)
      expect(msg.topic).to eql('logstash.event')

      payload = JSON.parse(msg.value)
      expect(payload['message']).to eql('foo-bar')
      expect(payload['@timestamp']).to be_kind_of(String)
    end
  end

  agent {}
end
