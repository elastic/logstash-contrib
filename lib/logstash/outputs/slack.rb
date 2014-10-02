# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Slack < LogStash::Outputs::Base
  config_name "slack"
  milestone 1

  # The incoming webhook URI needed to post a message
  config :url, :validate => :string, :required => true

  # The text to post in slack
  config :format, :validate => :string, :default => "%{message}"

  # The channel to post to
  config :channel, :validate => :string

  # The username to use for posting
  config :username, :validate => :string

  # Emoji icon to use
  config :icon_emoji, :validate => :string

  # Icon URL to use
  config :icon_url, :validate => :string


  public
  def register
    require 'ftw'
    require 'cgi'
    require 'json'

    @agent = FTW::Agent.new

    @content_type = "application/x-www-form-urlencoded"
  end # def register

  public
  def receive(event)
    return unless output?(event)

    payload_json = Hash.new
    payload_json['text'] = event.sprintf(@format)

    if not @channel.nil?
      payload_json['channel'] = @channel
    end

    if not @username.nil?
      payload_json['username'] = @username
    end

    if not @icon_emoji.nil?
      payload_json['icon_emoji'] = @icon_emoji
    elsif not @icon_url.nil?
      payload_json['icon_url'] = @icon_url
    end

    begin
      request = @agent.post(@url)
      request["Content-Type"] = @content_type
      request.body = "payload=#{CGI.escape(JSON.dump(payload_json))}"

      response = @agent.execute(request)

      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
      #puts rbody
    rescue Exception => e
      @logger.warn("Unhandled exception", :request => request,
                   :response => response, :exception => e,
                   :stacktrace => e.backtrace)
    end
  end # def receive
end # class LogStash::Outputs::Slack
