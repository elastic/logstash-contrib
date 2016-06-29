# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# This output plugin allows you to pull events from your logs and ship them to
# ganglia's event json file in the webapp.
# Checkout http://ganglia.info/?p=382 for how to enable overlay events in ganglia.
# https://logstash.jira.com/browse/LOGSTASH-2215 for "support" on this plugin

class LogStash::Outputs::GangliaEvent < LogStash::Outputs::Base

  config_name "ganglia_event"
  milestone 1

  # The address of the ganglia web server.
  config :ganglia_url, :validate => :string, :default => "http://localhost"

  # The port to connect on your ganglia web server.
  config :ganglia_port, :validate => :number, :default => 8649

  # The url to reach the event.php processor.
  config :ganglia_event_link, :validate => :string, :default => "ganglia/api/events.php"

  # You can specify now or a unix time stamp
  config :start_time, :validate => :string, :default => "now"

  # You can optionaly specify an end time
  config :end_time, :validate => :string, :default => nil

  # Event summary (what will labels of the graph)
  config :summary, :validate => :string, :required => true

  # Event description
  config :description, :validate => :string, :default => nil

  # Hosts the event applies to (it can be regular expression ie. web or web-0[2,4,5])
  config :host_regex, :validate => :string, :required => true

  # You can add a grid 
  config :grid, :validate => :string, :default => nil

  # You can specify a cluster
  config :cluster, :validate => :string, :default => nil
 

  
  public
  def register
    require "ftw"
    require "net/http"
  end # def register

  public
  def receive(event)
    begin
      agent = FTW::Agent.new
      options = {
        "start_time"  => @start_time,
        "summary"     => @summary,
        "description" => @description,
        "host_regex"  => @host_regex,
        "grid"        => @grid,
        "cluster"     => @cluster,
        "end_time"    => @end_time,
        "action"      => "add"
      }.reject { |k,v| v.nil? } 
      params = URI.encode_www_form(options)
      uri = "#{@ganglia_url}:#{@ganglia_port}/#{@ganglia_event_link}?#{params}"
      response = agent.get!(uri).read
 
      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
    rescue Exception => e
      @logger.warn("Unhandled exception", :uri => uri, :response => response, :exception => e, :stacktrace => e.backtrace)
    end
  end # def receive

end
