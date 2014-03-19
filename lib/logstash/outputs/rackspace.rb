# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Rackspace < LogStash::Outputs::Base

  config_name "rackspace"
  milestone 1

  default :codec, "json"

  # Rackspace Cloud Username
  config :username, :validate => :string, :required => true
  
  # Rackspace Cloud API Key
  config :api_key, :validate => :string, :required => true
   
  # Rackspace region
  # ord, dfw, lon, syd, etc
  config :region, :validate => :string, :default => 'dfw'

  # Rackspace Queue Name
  config :queue,  :validate => :string, :default => 'logstash'

  # time for item to live in queue
  config :ttl,    :validate => :number, :default => 360
  
  public
  def register
    require "fog"
    @service = Fog::Rackspace::Queues.new(
      :rackspace_username  => @username,   # Your Rackspace Username
      :rackspace_api_key   => @api_key,         # Your Rackspace API key
      :rackspace_region    => @region.to_sym,                  # Your desired region
      :connection_options  => {}                     # Optional connection options
    )

    begin
      @rackspace_queue = @service.queues.create :name => @queue
    rescue Fog::Rackspace::Queues::ServiceError => e
      if e.status_code == 204
        @logger.warn("Queue #{@queue} already exists")
      else
        @logger.warn("something bad happened!")
      end # rescue
    end # begin
    @service.queues.each_with_index do |queue, index|
      if queue.name == @queue
        @rackspace_queue = @service.queues[index]
        break
      end
    end
    @logger.info("Opened connection to rackspace cloud queues")
  end # def register

  public
  def receive(event)
    return unless output?(event)

    begin
      @rackspace_queue.messages.create :body => event, :ttl => @ttl
      #@rackspace_queue.messages.create :body => "some data here", :ttl => @ttl
    rescue => e
      @logger.warn("Failed to send event to rackspace cloud queues", :event => event, :exception => e,
                   :backtrace => e.backtrace)
    end

  end # def receive
end # class LogStash::Outputs::Rackspace
