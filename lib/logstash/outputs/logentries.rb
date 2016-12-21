require "logstash/outputs/base"
require "logstash/namespace"
require "uri"
require "net/http"
require "net/https"

# www.logentries.com

class LogStash::Outputs::Logentries < LogStash::Outputs::Base
  config_name "logentries"
  milestone 2

  # You will read this token from your config file. Just put the following lines to your .config file:
  #
  # output {
  #     logentries{
  #     token => "LOGENTRIES_TOKEN"
  #               }
  # }

  config :token, :validate => :string, :required => true

  public
  def register
  end

  def receive(event)
    return unless output?(event)

    if event == LogStash::SHUTDOWN
      finished
    return
    end

    # Send the event using token
    url = URI.parse("https://js.logentries.com/v1/logs/#{event.sprintf(@token)}")

    # Debug the URL here
    @logger.info("Sending using #{event.sprintf(@token)} Logentries Token")
    
    # Open HTTP connection
    http = Net::HTTP.new(url.host, url.port)

    # Use secure SSL
    if url.scheme == 'https'
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end

    request = Net::HTTP::Post.new(url.path)

    #Prepend the message body with "event" to allow the js.logentries to pick it up
    request.body = "{\"event\":" + event.to_json + "}"
    response = http.request(request)

    if response.is_a?(Net::HTTPSuccess)
      @logger.info("Event Sent!")
    else
      @logger.warn("HTTP error", :error => response.error!)
    end

  end # receive
end #LogStash::Outputs::Logentries
