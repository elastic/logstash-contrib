# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "open-uri"
require "sinatra/base"
require "base64"

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
    @cached_certs = {}
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
        if request.env.include? "HTTP_X_AMZ_SNS_MESSAGE_TYPE"
          msg = JSON.load(request.body)
          if plugin.verify_signature(msg)
            case request.env["HTTP_X_AMZ_SNS_MESSAGE_TYPE"]
            when "Notification"
              plugin.handle_message(msg)
            when "SubscriptionConfirmation"
              plugin.confirm_subscription(msg)
            when "UnsubscribeConfirmation"
              plugin.logger.warn("Got UnsubscribeConfirmation message for topic", :topic_arn => env["HTTP_X_AMZ_TOPIC_ARN"])
            else
              # Probably not a valid SNS request
              plugin.logger.info("Got an invalid message")
              400
            end
          else
            plugin.logger.warn("Message signature verification failed", :topic_arn => env["HTTP_X_AMZ_TOPIC_ARN"])
            401
          end
        else
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

  # TODO: Verify the certificate issuer signature. We may need to preload logstash with Verisign certificates
  def verify_signature(msg)
    raise RuntimeError, "Signature version not supported" unless msg['SignatureVersion'] == '1'
    signature_decoded = Base64.decode64(msg['Signature'])
    cert = get_cert(msg['SigningCertURL'])
    if cert.not_after < Time.now
      @logger.warn("SNS Certificate has expired", :cert_expiry => cert.not_after, :cert_subject => cert.subjet.to_s)
      return false
    end
    return false unless cert.subject.to_a.find{|c| c.first == "CN"}[1] == "sns.amazonaws.com"
    cert.public_key.verify(OpenSSL::Digest::SHA1.new, signature_decoded, str_to_sign(msg))
  rescue Exception => e
    @logger.error("Exception while verifying SNS message signature", :exception => e)
    false
  end

  private
  def str_to_sign(msg)
    signature_fields = case msg['Type']
    when 'Notification'
      ['Message', 'MessageId', 'Subject', 'Timestamp', 'TopicArn', 'Type'].sort
    when 'SubscriptionConfirmation', "UnsubscribeConfirmation"
      ['Message', 'MessageId', 'SubscribeURL', 'Timestamp', 'Token', 'TopicArn', 'Type'].sort
    end
    canonical_str = signature_fields.map{|field| [field, msg[field]] if msg[field] }.compact.flatten.join("\n")
    canonical_str += "\n"
    return canonical_str
  end

  def get_cert(cert_name)
    unless @cached_certs.include? cert_name
        cert_body = open(cert_name).read
        cert = OpenSSL::X509::Certificate.new(cert_body)
        @cached_certs[cert_name] = cert
    end
    @cached_certs[cert_name]
  end
end # class LogStash::Inputs::SNS
