# encoding: utf-8
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"
require "logstash/util/socket_peer"
require "socket"
require "timeout"

# Read events over a TCP socket from a Log4j2 SocketAppender.
#
# Can either accept connections from clients or connect to a server,
# depending on `mode`. Depending on which `mode` is configured,
# you need a matching SocketAppender or a SocketHubAppender
# on the remote side.
class LogStash::Inputs::Log4j2 < LogStash::Inputs::Base

  config_name "log4j2"
  milestone 1

  # When mode is `server`, the address to listen on.
  # When mode is `client`, the address to connect to.
  config :host, :validate => :string, :default => "0.0.0.0"

  # When mode is `server`, the port to listen on.
  # When mode is `client`, the port to connect to.
  config :port, :validate => :number, :default => 4560

  # Read timeout in seconds. If a particular TCP connection is
  # idle for more than this timeout period, we will assume
  # it is dead and close it.
  # If you never want to timeout, use -1.
  config :data_timeout, :validate => :number, :default => 5

  # Mode to operate in. `server` listens for client connections,
  # `client` connects to a server.
  config :mode, :validate => ["server", "client"], :default => "server"

  def initialize(*args)
    super(*args)
  end # def initialize

  public
  def register
    require "java"
    require "jruby/serialization"

    begin
      if __FILE__ !~ /^(jar:)?file:\/\//
        if File.exists?(File.dirname(__FILE__)+"/log4j-api-2.0.jar")
          require File.dirname(__FILE__)+"/log4j-api-2.0.jar"
        end
        if File.exists?(File.dirname(__FILE__)+"/log4j-core-2.0.jar")
          require File.dirname(__FILE__)+"/log4j-core-2.0.jar"
        end
      end
      Java::OrgApacheLoggingLog4jCoreImpl.const_get("Log4jLogEvent")
    rescue
       raise(LogStash::PluginLoadingError, "Log4j2 java library not loaded")
    end

    if server?
      @logger.info("Starting Log4j2 input listener", :address => "#{@host}:#{@port}")
      @server_socket = TCPServer.new(@host, @port)
    end
    @logger.info("Log4j input")
  end # def register

  private 
  def prettyPrintEntireStackTrace(proxy)
    indentation = "\n\t"
    a = proxy.getName + " " + proxy.getMessage + indentation
    a += proxy.getExtendedStackTrace.to_a.join(indentation)
    while (b = proxy.getCauseProxy)
      a += "\nCaused by: " + b.getName + " " + b.getMessage + indentation + b.getExtendedStackTrace.to_a.join(indentation)
      proxy = b 
    end        
    return a
  end

  private
  def handle_socket(socket, output_queue)
    begin     
    
      # JRubyObjectInputStream uses JRuby class path to find the class to de-serialize to
      ois = JRubyObjectInputStream.new(java.io.BufferedInputStream.new(socket.to_inputstream))
      loop do
        # NOTE: log4j_obj is org.apache.logging.log4j.core.impl.Log4jLogEvent
        log4j_obj = ois.readObject
        event = LogStash::Event.new("message" => log4j_obj.getMessage.getFormattedMessage, LogStash::Event::TIMESTAMP => Time.at(log4j_obj.getTimeMillis/1000,log4j_obj.getTimeMillis%1000*1000).gmtime)
        decorate(event)
        event["host"] = socket.peer
        event["marker"] = log4j_obj.getMarker.getName if log4j_obj.getMarker
        event["path"] = log4j_obj.getLoggerName
        event["priority"] = log4j_obj.getLevel.toString
        event["logger_name"] = log4j_obj.getLoggerName
        event["thread"] = log4j_obj.getThreadName
        event["class"] = log4j_obj.getSource().getClassName
        event["file"] = log4j_obj.getSource().getFileName + ":" + log4j_obj.getSource().getLineNumber.to_s
        event["method"] = log4j_obj.getSource().getMethodName
        # Add the context properties to '@fields'
        if log4j_obj.contextMap
          log4j_obj.contextMap.keySet.each do |key|
            event["cmap_"+key] = log4j_obj.contextMap.get(key)
          end  
        end

        proxy = log4j_obj.getThrownProxy
        if proxy
          event["stack_trace"] = prettyPrintEntireStackTrace(proxy)
        end

        event["cstack"] = log4j_obj.getContextStack.to_a if log4j_obj.getContextStack  
        output_queue << event
      end # loop do
    rescue => e
      @logger.debug(e)
      @logger.debug("Closing connection", :client => socket.peer,
                    :exception => e)
    rescue Timeout::Error
      @logger.debug("Closing connection after read timeout",
                    :client => socket.peer)
    end # begin
  ensure
    begin
      socket.close
    rescue IOError
      pass
    end # begin
  end

  private
  def server?
    @mode == "server"
  end # def server?

  private
  def readline(socket)
    line = socket.readline
  end # def readline

  public
  def run(output_queue)
    if server?
      loop do
        # Start a new thread for each connection.
        Thread.start(@server_socket.accept) do |s|
          # TODO(sissel): put this block in its own method.

          # monkeypatch a 'peer' method onto the socket.
          s.instance_eval { class << self; include ::LogStash::Util::SocketPeer end }
          @logger.debug("Accepted connection", :client => s.peer,
                        :server => "#{@host}:#{@port}")
          handle_socket(s, output_queue)
        end # Thread.start
      end # loop
    else
      loop do
        client_socket = TCPSocket.new(@host, @port)
        client_socket.instance_eval { class << self; include ::LogStash::Util::SocketPeer end }
        @logger.debug("Opened connection", :client => "#{client_socket.peer}")
        handle_socket(client_socket, output_queue)
      end # loop
    end
  end # def run
end # class LogStash::Inputs::Log4j2
