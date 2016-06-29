# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "sinatra/base"

# A simple HTTP input, recieves events via HTTP POST body
# Does not provide Content-Type negotiation nor SSL termination.
class LogStash::Inputs::HTTP < LogStash::Inputs::Base
  config_name "http"
  milestone 1

  default :codec, "json"

  # The url to connect to or serve from
  config :port, :validate => :number, :default => 9990

  # The address to bind (listen) on
  config :bind_address, :validate => :string, :default => "0.0.0.0"

  def register
    require "rack/handler/puma"
  end # def register

  attr_reader :logger

  public
  def run(output_queue)
    @output_queue = output_queue
    # bind self to a variable so we can reference it from whithin the sinatra handler
    plugin = self 
    handler = Sinatra.new do
      post("/*") do
        request.body.rewind
        plugin.handle_message(request.body.read, request)
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

  def handle_message(msg, request)
    codec.decode(msg) do |event|
      event["source_host"] = request.ip unless event.include?("source_host")
      event["http"] = {
        "path" => request.path,
        "remote_address" => request.ip,
        "user_agent" => request.user_agent
      }
      event["http"]["query_string"] = request.query_string unless request.query_string.empty?
      event["http"]["x-forwarded-for"] = request.env["HTTP_X_FORWARDED_FOR"]
      decorate(event)
      @output_queue << event
    end
  end

end # class LogStash::Inputs::HTTP
