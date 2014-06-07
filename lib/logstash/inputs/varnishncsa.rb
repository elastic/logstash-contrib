# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket" # for Socket.gethostname

# Stream Varnish logs in Apache / NCSA combined log format
# https://www.varnish-cache.org/docs/3.0/reference/varnishncsa.html
class LogStash::Inputs::Varnishncsa < LogStash::Inputs::Base
  DEFAULT_FORMAT =
  '{"@timestamp":"%{%Y-%m-%dT%H:%M:%S%z}t","request_url":"%U","response_time_microseconds":"%D",
  "response_time_seconds":"%T","request_protocol":"%H","response_size":"%b",
  "request_method":"%m","query_string":"%q","message":"%r","status":"%s","http_user":"%u",
  "request_header_x_forwarded_for":"%{X-Forwarded-For}i","request_header_user_agent":"%{User-agent}i",
  "request_header_accept_language":"%{Accept-Language}i","request_header_host":"%{Host}i","request_header_referer":"%{Referer}i",
  "response_header_cache_control":"%{Cache-Control}o","response_header_content_encoding":"%{Content-Encoding}o",
  "response_header_content_type":"%{Content-Type}o","response_header_last_modified":"%{Last-Modified}o",
  "varnish_time_firstbyte":"%{Varnish:time_firstbyte}x","varnish_hitmiss":"%{Varnish:hitmiss}x",
  "varnish_handling":"%{Varnish:handling}x"}'

  config_name "varnishncsa"
  milestone 1

  default :codec, "json"

  # Set the format for varnishncsa
  #
  #  'default' will use the following output:
  #     '{"@timestamp":"%{%Y-%m-%dT%H:%M:%S%z}t","request_url":"%U","response_time_microseconds":"%D",
  #     "response_time_seconds":"%T","request_protocol":"%H","response_size":"%b",
  #     "request_method":"%m","query_string":"%q","message":"%r","status":"%s","http_user":"%u",
  #     "request_header_x_forwarded_for":"%{X-Forwarded-For}i","request_header_user_agent":"%{User-agent}i",
  #     "request_header_accept_language":"%{Accept-Language}i","request_header_host":"%{Host}i","request_header_referer":"%{Referer}i",
  #     "response_header_cache_control":"%{Cache-Control}o","response_header_content_encoding":"%{Content-Encoding}o",
  #     "response_header_content_type":"%{Content-Type}o","response_header_last_modified":"%{Last-Modified}o",
  #     "varnish_time_firstbyte":"%{Varnish:time_firstbyte}x","varnish_hitmiss":"%{Varnish:hitmiss}x",
  #     "varnish_handling":"%{Varnish:handling}x"}'
  #
  #   or put whatever format you like that suppoted by varnishncsa:
  #   https://www.varnish-cache.org/docs/3.0/reference/varnishncsa.html
  #
  config :format, :validate => :string, :required => true, :default => 'default'
  
  public
  def register
    @logger.info("Registering varnishncsa input", :format => @format)
    @format = case @format
              when 'default'
                DEFAULT_FORMAT.gsub(/\n\s/,'')
              else
                @format
              end

    command = "varnishncsa -F '#{@format}'"
    @pipe = IO.popen(command, mode="r")
  end # def register

  def teardown
    @pipe.close if @pipe
  end

  def run(queue)
    loop do
      begin
        hostname = Socket.gethostname
        @pipe.each do |line|
          @codec.decode(line) do |event|
            event["source_host"] = hostname
            decorate(event)
            queue << event
          end
        end
      rescue LogStash::ShutdownSignal => e
        break
      rescue Exception => e
        @logger.error("Exception while running varnishncsa", :e => e, :backtrace => e.backtrace)
      ensure
        @pipe.close
      end

      sleep(1)
    end
  end # def run
end # class LogStash::Inputs::Varnishncsa
