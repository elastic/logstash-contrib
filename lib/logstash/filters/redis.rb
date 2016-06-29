# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# Redis filter. This is used to store/retrieve information from redis server
# and populate a field from it.
#
# For more information about Redis, see <http://redis.io/>
#
# The config looks like this:
#
#     filter {
#       redis {
#         command => "SET"
#         key => "my-key"
#         value => "1234"
#       }
#     }
#
#   or
#
#     filter {
#       redis {
#         command => "GET"
#         key => "my-key"
#         field => "read-from-redis"
#       }
#     }
#
class LogStash::Filters::Redis < LogStash::Filters::Base
  config_name "redis"
  milestone 2

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Initial connection timeout in seconds.
  config :timeout, :validate => :number, :default => 5

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The name of a Redis key.
  config :key, :validate => :string, :required => true

  # The value to store (SET).
  config :value, :validate => :string, :required => false

  # The name of a Redis value.
  config :command, :validate => :string, :required => true, :default => "GET"

  # The name of a Redis value.
  config :field, :validate => :string, :required => false

  public
  def register
    require 'redis'
    @redis = nil
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

    if @command == "GET" and @field.nil?
      raise "field is mandatory is redis command is 'GET'"
    end

    if @command != "GET" and @command != "SET"
      raise "Redis command must be either 'GET' or 'SET'"
    end

    @logger.info("Registering Redis", :identity => identity)
  end # def register

  # A string used to identify a Redis instance in log messages
  # TODO(sissel): Use instance variables for this once the @name config
  # option is removed.
  private
  def identity
    @name || "#{@redis_url} #{@data_type}:#{@key}"
  end

  private
  def connect
    redis = Redis.new(
      :host => @host,
      :port => @port,
      :timeout => @timeout,
      :db => @db,
      :password => @password.nil? ? nil : @password.value
    )
    return redis
  end # def connect

  public
  def filter(event)
    return unless filter?(event)
	
	begin
		@redis ||= connect
		if @command == "SET"
			@redis.set event.sprintf(@key), event.sprintf(@value)
		else # if @command == "GET"
			event[event.sprintf(@field)] = @redis.get event.sprintf(@key)
		end		
	rescue Redis::CannotConnectError => e
		@logger.warn("Redis connection problem", :exception => e)
#		sleep 1
#		@redis = connect
	rescue => e # Redis error
		@logger.warn("Failed to send command to Redis", :name => @name,
					 :exception => e, :backtrace => e.backtrace)
		raise e
	end
	
    
    filter_matched(event)
  end # def filter

  public
  def teardown
    @redis = nil
  end
end # class LogStash::Filters::Redis
