# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/socket_peer"

# Read memcached_stats over a TCP socket.
#
# Like stdin and file inputs, each event is assumed to be one line of text.
#
class LogStash::Inputs::MemcachedStats < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "memcached_stats"
  milestone 1

  default :codec, "line"

  # When mode is `server`, the address to listen on.
  # When mode is `client`, the address to connect to.
  config :host, :validate => :string, :default => "localhost"

  # When mode is `server`, the port to listen on.
  # When mode is `client`, the port to connect to.
  config :port, :validate => :number,  :default => 11211

  # The polling period for talking to memcached to issue the stats command, default 1
  config :poll_period_s, :validate => :number, :default => 1

  # The time (seconds) to wait before attempting to connect to memcached again, default 5
  config :reconnect_period_s, :validate => :number, :default => 5

  # Stats separator.  This is the separate used to separate the different stats.  default is a pipe character
  config :stat_separator, :validate => :string, :default => "|"

  # The separator used between the key and the value
  config :value_separator, :validate => :string, :default => "="

  # store all as events.  The default is false.  When true all the memcached stats will be stored as events
  config :store_all_keys, :validate => :boolean, :default => false

  # keys to not store
  config :ignore_keys, :validate => :array, :default => Array



  def initialize(*args)
    super(*args)
  end # def initialize

  public
  def register
    # not available in logstash 1.3.3
    #fix_streaming_codecs
    require "socket"
    require "timeout"   
  end # def register

  private
  def handle_socket(socket, client_address, output_queue, codec)
    notintegerstats = { "libevent" => "1", "version" => "1"}
    while true
      socket.puts("stats")      
      statsMap = read(socket)
      calculatePercentages(statsMap)
      line = statsToMessageLine(statsMap)
      hostname = Socket.gethostname

      codec.decode(line) do |event|
        if(@store_all_keys)
          statsMap.sort.map { |k,v|
            if(!@ignore_keys.include?(k))
              if(notintegerstats.has_key?(k))
                event[k] ||= v
              else
                event[k] ||= v.to_i
              end
            end
          }
        end
        event["host"] = hostname if !event.include?("host")
        event["memcachedhost"] ||= @host
        event["memcachedport"] ||= @port
        decorate(event)
        output_queue << event
      end

      sleep(@poll_period_s)
    end # loop do
  rescue LogStash::ShutdownSignal => e
    raise e
  rescue EOFError
    @logger.debug("Connection closed", :client => socket.peer)
  rescue => e
    @logger.debug("An error occurred. Closing connection",
                  :client => socket.peer, :exception => e, :backtrace => e.backtrace)
  ensure
    socket.close rescue IOError nil
    hostname = Socket.gethostname
    codec.respond_to?(:flush) && codec.flush do |event|
      event["host"] = hostname if !event.include?("host")
      event["memcachedhost"] ||= @host
      event["memcachedport"] ||= @port      
      decorate(event)
      output_queue << event
    end
  end

  private 
  def closeSocket(client_socket)
    begin      
      unless client_socket.nil? or client_socket.empty?
        client_socket.close
      end
    rescue => e
      @logger.debug("Unable to close previous connection to memcached.  More than likely already closed", :name => @host,
                   :exception => e, :backtrace => e.backtrace)
    end
  end

  private
  def read(socket)
    stats = { }
    statsRead = true
    while statsRead
      line = socket.gets
      if(line.start_with?('END'))
        statsRead = false
      else
        line = line.chop
        line.gsub!(/STAT ([^\s]+) ([^\s]+)/,'\1=\2')
        keyValue = line.split('=')
        stats.store(keyValue[0],keyValue[-1])

      end
    end
    return stats
  end # def readline

  private
  def statsToMessageLine(stats)
    line = ""
    stats.sort.map {|k,v| line = line + @stat_separator + k + @value_separator + v  }
    return line + "|\n"
  end

  private
  def calculatePercentages(stats)
    percentageCalculate("hit_percent",stats["get_hits"],stats["get_misses"],stats)
    percentageCalculate("gets_to_sets_percent",stats["cmd_get"],stats["cmd_set"],stats)
  end

  private
  def percentageCalculate(key,hits,misses,stats)
    hitsf = hits.to_f
    total = misses.to_f + hitsf
    percent = 0
    if(total>0.0)
      percent = (hitsf/total) * 100
    end
    stats.store(key,percent.to_s)
  end

  public
  def run(output_queue)
      run_client(output_queue) 
  end # def run


  def run_client(output_queue) 
    @thread = Thread.current
    notfinished = true
    client_socket = nil
    while notfinished
      begin
        client_socket = TCPSocket.new(@host, @port)
        client_socket.instance_eval { class << self; include ::LogStash::Util::SocketPeer end }
        @logger.debug("Opened connection", :client => "#{client_socket.peer}")
        handle_socket(client_socket, client_socket.peer, output_queue, @codec.clone)
      rescue LogStash::ShutdownSignal
        notfinished = false
        closeSocket(client_socket)            
      rescue Exception => e # memcached connection error
        case e
          when Errno::ECONNREFUSED,Errno::ECONNABORTED,Errno::ECONNRESET
            @logger.warn("Failed to get stats from memcached, retrying connection in #{reconnect_period_s} seconds", :name => @host,
                       :exception => e, :backtrace => e.backtrace)
            closeSocket(client_socket)        
            sleep(reconnect_period_s)
          else
            raise e
        end
      end
    end # loop
  ensure
    closeSocket(client_socket)
  end # def run

  public
  def teardown    
  end # def teardown
end # class LogStash::Inputs::MemcachedStats
