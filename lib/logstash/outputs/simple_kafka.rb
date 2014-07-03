require 'logstash/outputs/base'
require 'logstash/namespace'
require 'poseidon'
require 'securerandom'

class LogStash::Outputs::SimpleKafka < LogStash::Outputs::Base
  config_name "simple_kafka"
  milestone 1

  default :codec, "json"

  # The address to send messages to
  config :host, :validate => :string, :required => true

  # The port to send messages on
  config :port, :validate => :number, :required => true

  # The topic to send the message to
  config :topic_id, :validate => :string, :required => true

  # The string to identify this client
  config :client_id, :validate => :string, :default => "logstash-#{SecureRandom.uuid}"

  public

  def register
    STDERR.puts "creating producer"

    @producer = Poseidon::Producer.new(["#{@host}:#{@port}"], @client_id)

    @codec.on_event do |payload|
      STDERR.puts "sending event"
      message = Poseidon::MessageToSend.new(@topic_id, payload)
      @producer.send_messages([message])
    end
  end

  public

  def receive(event)
    return unless output?(event)

    if event == LogStash::SHUTDOWN
      finished
      return
    end

    @codec.encode(event)
  end


end
