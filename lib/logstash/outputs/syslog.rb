# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "date"
require "openssl"


# Send events to a syslog server.
#
# You can send messages compliant with RFC3164 or RFC5424
# UDP or TCP syslog transport is supported
class LogStash::Outputs::Syslog < LogStash::Outputs::Base
  config_name "syslog"
  milestone 1

  FACILITY_LABELS = [
    "kernel",
    "user-level",
    "mail",
    "daemon",
    "security/authorization",
    "syslogd",
    "line printer",
    "network news",
    "uucp",
    "clock",
    "security/authorization",
    "ftp",
    "ntp",
    "log audit",
    "log alert",
    "clock",
    "local0",
    "local1",
    "local2",
    "local3",
    "local4",
    "local5",
    "local6",
    "local7",
  ]

  SEVERITY_LABELS = [
    "emergency",
    "alert",
    "critical",
    "error",
    "warning",
    "notice",
    "informational",
    "debug",
  ]

  # syslog server address to connect to
  config :host, :validate => :string, :required => true

  # syslog server port to connect to
  config :port, :validate => :number, :required => true

  # syslog server protocol. you can choose between UDP, TCP, or TLS over TCP
  config :protocol, :validate => ["tcp", "udp", "tls-tcp"], :default => "udp"

  # facility label for syslog message
  config :facility, :validate => FACILITY_LABELS, :required => true

  # severity label for syslog message
  config :severity, :validate => SEVERITY_LABELS, :required => true

  # source host for syslog message
  config :sourcehost, :validate => :string, :default => "%{host}"

  # timestamp for syslog message
  config :timestamp, :validate => :string, :default => "%{@timestamp}", :deprecated => "This setting is no longer necessary. The RFC setting will determine what time format is used."

  # application name for syslog message
  config :appname, :validate => :string, :default => "LOGSTASH"

  # process id for syslog message
  config :procid, :validate => :string, :default => "-"

  # message id for syslog message
  config :msgid, :validate => :string, :default => "-"

  # syslog message format: you can choose between rfc3164 or rfc5424
  config :rfc, :validate => ["rfc3164", "rfc5424"], :default => "rfc3164"


  public
  def register
    @client_socket = nil
    @last_message_sent = 0
    @num_retries = (Integer ENV['SYSLOG_MAX_RETRIES'] rescue nil) || 3
    @timeout = (Integer ENV['SYSLOG_CONNECTION_TIMEOUT_SECONDS'] rescue nil) || 30
  end

  private
  def rfc3164?
    @rfc == "rfc3164"
  end

  private
  def connect
    @client_socket.close rescue nil if @client_socket
    if @protocol == 'udp'
        @client_socket = UDPSocket.new
        @client_socket.connect(@host, @port)
    else
        @client_socket = TCPSocket.new(@host, @port)
        if @protocol == 'tls-tcp'
            ssl = OpenSSL::SSL::SSLContext.new
            ssl.verify_mode = OpenSSL::SSL::VERIFY_PEER
            cert_store = OpenSSL::X509::Store.new
            cert_store.set_default_paths
            ssl.cert_store = cert_store
            @client_socket = OpenSSL::SSL::SSLSocket.new(@client_socket, ssl)
            @client_socket.sync_close = true
        end
        @client_socket.connect
    end
  end

  public
  def receive(event)
    return unless output?(event)

    appname = event.sprintf(@appname)
    procid = event.sprintf(@procid)
    sourcehost = event.sprintf(@sourcehost)

    facility_code = FACILITY_LABELS.index(@facility)

    severity_code = SEVERITY_LABELS.index(@severity)

    priority = (facility_code * 8) + severity_code

    if rfc3164?
      timestamp = event.sprintf("%{+MMM dd HH:mm:ss}")
      syslog_msg = "<"+priority.to_s()+">"+timestamp+" "+sourcehost+" "+appname+"["+procid+"]: "+event["message"]
    else
      msgid = event.sprintf(@msgid)
      timestamp = event.sprintf("%{+YYYY-MM-dd'T'HH:mm:ss.SSSZ}")
      syslog_msg = "<"+priority.to_s()+">1 "+timestamp+" "+sourcehost+" "+appname+" "+procid+" "+msgid+" - "+event["message"]
    end

    @num_retries.times do |attempt|
      begin
        now = Time.now
        connect unless @client_socket && (now - @last_message_sent) < @timeout
        @client_socket.write(syslog_msg + "\n")
        @last_message_sent = now
        return
      rescue => e
        @logger.warn(@protocol+" output exception on attempt #{attempt}",
                     :host => @host, :port => @port,
                     :exception => e, :backtrace => e.backtrace)
        @client_socket.close rescue nil
        @client_socket = nil
      end
    end
  end
end
