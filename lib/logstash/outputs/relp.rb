# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/relp"
require "logstash/util/socket_peer"



# Write RELP events over a TCP socket.
#
# For more information about RELP, see 
# <http://www.rsyslog.com/doc/imrelp.html>
#
# This protocol implements application-level acknowledgements to help protect
# against message loss.
#
# Output will block as long as messages are not ack'ed
class LogStash::Outputs::Relp < LogStash::Outputs::Base

  config_name "relp"
  milestone 0

  default :codec, "plain"

  # The address to connect to.
  config :host, :validate => :string, :required => true

  # The port to connect to.
  config :port, :validate => :number, :required => true

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  def initialize(*args)
    super(*args)
  end # def initialize

  private
  def connect
    @logger.info("Starting relp output listener", :address => "#{@host}:#{@port}")
    return RelpClient.new(@host, @port, ['syslog'], 128, @retry_delay)
  end # def connect

  public
  def register
    @codec.on_event do |event|
      begin
        @relp_client = connect unless @relp_client
        @relp_client.syslog_write(event)
      rescue Relp::ConnectionClosed, IOError => e
        @logger.warn("Failed to send event to RelpServer", :event => event, :exception => e,
                     :backtrace => e.backtrace)

        begin
          @relp_client.close rescue nil
        rescue Relp::ConnectionClosed, IOError => e
        end

        @relp_client = nil
        sleep @retry_delay
        retry
      rescue Relp::InvalidCommand,Relp::InappropriateCommand => e
        @logger.warn('Relp server trying to open connection with something other than open:'+e.message)
        raise
      rescue Relp::InsufficientCommands
        @logger.warn('Relp server incapable of syslog')
        raise
      rescue Relp::RelpError => e
        @logger.warn('Relp error: '+e.class.to_s+' '+e.message)
        raise
      end
    end
  end # def register

  def teardown
    begin
      @relp_client.close rescue nil
    rescue Relp::ConnectionClosed, IOError => e
    end
  end # def teardown

  public
  def receive(event)
    return unless output?(event)

    @codec.encode(event)
  end # def receive
end # class LogStash::Outputs::Mongodb
