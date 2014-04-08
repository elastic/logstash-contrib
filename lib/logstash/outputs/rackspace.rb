# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"

class LogStash::Outputs::Rackspace < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "rackspace"
  milestone 1

  # Rackspace Cloud Username
  config :username, :validate => :string, :required => true
  
  # Rackspace Cloud API Key
  config :api_key, :validate => :string, :required => true
   
  # Rackspace region
  # ord, dfw, lon, syd, etc
  config :region, :validate => :string, :default => 'dfw'

  # Rackspace queue name
  #
  # The name MUST NOT exceed 64 bytes in length, and is limited to
  # US-ASCII letters, digits, underscores and hyphens.
  #
  # Note that there is a bug in Fog versions up to and including
  # 1.21.0 that prevents usage with queue names that include a hyphen.
  config :queue,  :validate => :string, :default => 'logstash'

  # Time for item to live in queue
  # Min: 60, Max: 1209600 (14 days)
  config :ttl,    :validate => :number, :default => 60
  
  # To make efficient api calls, we will buffer a certain number of
  # events before flushing that out to Rackspace. This setting
  # controls how many events will be buffered before sending a batch
  # of events.
  config :flush_size, :validate => :number, :default => 10

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # Logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  public
  def register
    require "fog"
    @service = Fog::Rackspace::Queues.new(
      :rackspace_username  => @username,   # Your Rackspace Username
      :rackspace_api_key   => @api_key,         # Your Rackspace API key
      :rackspace_region    => @region.to_sym,                  # Your desired region
      :connection_options  => {}                     # Optional connection options
    )

    @logger.info("Opened connection to rackspace cloud queues")

    @queues = Hash.new

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end # def register

  public
  def receive(event)
    return unless output?(event)

    queue_name = event.sprintf(@queue)
    buffer_receive({:body => event.to_hash, :ttl => @ttl}, :queue_name => queue_name )
  end # def receive

  def get_queue(queue_name)
    if @queues.has_key?(queue_name)
      return @queues[queue_name]
    end # if

    @logger.warn("Using #{queue_name} for the first time")

    begin
      @service.queues.create :name => queue_name
    rescue Fog::Rackspace::Queues::ServiceError => e
      if e.status_code == 204
        @logger.warn("Queue #{queue_name} already exists")
      else
        @logger.warn("something bad happened!")
      end # if
    end # begin
    @service.queues.each_with_index do |queue, index|
      if queue.name == queue_name
        queue = @service.queues[index]
        @queues[queue_name] = queue
        return queue
      end # if
    end # do

    return nil
  end # def get_queue

  def flush(messages, group, teardown=false)
    # Fog is not currently able to post multiple messages in one API
    # call. Until it can, post each message individually.
    queue = get_queue(group[:queue_name])
    messages.each do |message|
      begin
        queue.messages.create message
      rescue => e
        @logger.warn("Failed to send event to Rackspace cloud queues",
                     :message => message,
                     :queue_name => group[:queue_name],
                     :exception => e,
                     :backtrace => e.backtrace)
      end # begin
    end # do
  end # def flush

  def teardown
    buffer_flush(:final => true)
  end

end # class LogStash::Outputs::Rackspace
