# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "open-uri"
require "sinatra/base"

# Recieve events from an SNS topic
class LogStash::Inputs::SNS < LogStash::Inputs::Base
  config_name "sns"
  milestone 1

  default :codec, "json"

  # The url to connect to or serve from
  config :port, :validate => :number, :default => 9991

  config :bind_address, :validate => :string, :default => "0.0.0.0"

  def register
    require "rack/handler/puma"
    require "nokogiri"
  end # def register

  attr_reader :logger

  public
  def run(output_queue)
    @output_queue = output_queue
    # bind self to variable so we can reference it from whithin the sinatra handler
    plugin = self 
    handler = Sinatra.new do
      post("/") do
        request.body.rewind
        case request.env["HTTP_X_AMZ_SNS_MESSAGE_TYPE"]
        when "Notification"
          plugin.handle_message(JSON.load(request.body))
          200
        when "SubscriptionConfirmation"
          plugin.confirm_subscription(JSON.load(request.body))
          200
        else
          # Probably not a valid SNS request
          plugin.logger.info("Got an invalid message")
          400
        end
      end
    end
    Rack::Handler::Puma.run(handler, :Host => @bind_address, :Port => @port) do |server|
      @server = server
    end
  end # def run

  def teardown
    @server.stop(true)
    finished
  end

  def handle_message(msg)
    codec.decode(msg["Message"]) do |event|
      event["sns"] = {
                        "topic_arn" => msg["TopicArn"],
                        "message_id" => msg["MessageId"],
                        "timestamp" => msg["Timestamp"]
                      }
      event["sns"]["subject"] = msg["Subject"] if msg["Subject"]
      decorate(event)
      @output_queue << event
    end
  end

  def confirm_subscription(subscription_msg)
    @logger.debug("Got a subscription confirmation message", :msg => subscription_msg)
    r = open(subscription_msg["SubscribeURL"])
    if r.status.first == "200"
      doc = Nokogiri::XML(r)
      subscription_arn = doc.css("SubscriptionArn").first
      @logger.warn("Successfully subscribed to SNS topic",
        :subscription_arn => subscription_arn,
        :topic_arn => subscription_msg["TopicArn"])
    else
      @logger.warn("SNS Subscription confirmation failed", :subscription_message => subscription_msg)
    end
  rescue Exception => e
    @logger.warn("Failed requesting subscribition confirmation URL", :exception => e)
  end

end # class LogStash::Inputs::SNS
