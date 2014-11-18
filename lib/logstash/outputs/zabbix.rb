# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "shellwords"

# The zabbix output is used for sending item data to zabbix via the
# zabbix_sender executable.
#
# For this output to work, your event must have the following fields:
#
# * "zabbix_host"    (the host configured in Zabbix)
# * "zabbix_item"    (the item key on the host in Zabbix)
# * "send_field"    (the field name that is sending to Zabbix)
#
# In Zabbix, create your host with the same name (no spaces in the name of
# the host supported) and create your item with the specified key as a
# Zabbix Trapper item. Also you need to set field that will be send to zabbix
# as item.value, otherwise @message wiil be sent.
#
# The easiest way to use this output is with the grep filter.
# Presumably, you only want certain events matching a given pattern
# to send events to zabbix, so use grep or grok to match and also to add the required
# fields.
#
#      filter {
#        grep {
#          type => "linux-syslog"
#          match => [ "@message", "(error|ERROR|CRITICAL)" ]
#          add_tag => [ "zabbix-sender" ]
#          add_field => [
#            "zabbix_host", "%{source_host}",
#            "zabbix_item", "item.key"
#            "send_field", "field_name"
#          ]
#       }
#        grok {
#          match => [ "message", "%{SYSLOGBASE} %{DATA:data}" ]
#          add_tag => [ "zabbix-sender" ]
#          add_field => [
#            "zabbix_host", "%{source_host}",
#            "zabbix_item", "item.key",
#            "send_field", "data"
#          ]
#       }
#     }
#
#     output {
#       zabbix {
#         # only process events with this tag
#         tags => "zabbix-sender"
#
#         # specify the hostname or ip of your zabbix server
#         # (defaults to localhost)
#         host => "localhost"
#
#         # specify the port to connect to (default 10051)
#         port => "10051"
#
#         # specify the path to zabbix_sender
#         # (defaults to "/usr/local/bin/zabbix_sender")
#         zabbix_sender => "/usr/local/bin/zabbix_sender"
#       }
#     }
class LogStash::Outputs::Zabbix < LogStash::Outputs::Base

  config_name "zabbix"
  milestone 2

  config :host, :validate => :string, :default => "localhost"
  config :port, :validate => :number, :default => 10051
  config :zabbix_sender, :validate => :path, :default => "/usr/local/bin/zabbix_sender"

  public
  def register
    # nothing to do
  end # def register

  public
  def receive(event)
    return unless output?(event)

    if !File.exists?(@zabbix_sender)
      @logger.warn("Skipping zabbix output; zabbix_sender file is missing",
                   :zabbix_sender => @zabbix_sender, :missed_event => event)
      return
    end

    host = Array(event["zabbix_host"])
    if host.empty?
      @logger.warn("Skipping zabbix output; zabbix_host field is missing",
                   :missed_event => event)
      return
    end

    item = Array(event["zabbix_item"])
    if item.empty?
      @logger.warn("Skipping zabbix output; zabbix_item field is missing",
                   :missed_event => event)
      return
    end

    field = Array(event["send_field"])
    if field.empty?
      field = ["message"]
    end

    item.each_with_index do |key, index|

      if field[index].nil? || (zmsg = event[field[index]]).nil?
        @logger.warn("No zabbix message to send in event field #{field[index].inspect}", :field => field, :index => index, :event => event)
        next
      end

      cmd = [@zabbix_sender, "-z", @host, "-p", @port, "-s", host[index].to_s, "-k", item[index].to_s, "-o", zmsg.to_s, "-v"]

      @logger.debug("Running zabbix command", :command => cmd.join(" "))

      begin
        f = IO.popen(cmd, "r")

        command_output = f.gets
        command_processed = command_output[/processed: (\d+)/, 1]
        command_failed = command_output[/failed: (\d+)/, 1]
        command_total = command_output[/total: (\d+)/, 1]
        command_seconds_spent = command_output[/seconds spent: ([\d\.]+)/, 1]

        @logger.info("Message was sent to zabbix server",
                     :command => cmd, :event => event,
                     :command_processed => command_processed,
                     :command_failed => command_failed,
                     :command_total => command_total,
                     :command_seconds_spent => command_seconds_spent)
      rescue => e
        @logger.warn("Skipping zabbix output; error calling zabbix_sender",
                     :command => cmd, :missed_event => event,
                     :exception => e, :backtrace => e.backtrace)
      ensure
        begin
          f.close unless f.closed?
        rescue => e
          @logger.warn("Error during closing zabbix_sender subprocess",
                     :exception => e, :backtrace => e.backtrace)
        end
      end
    end
  end # def receive
end # class LogStash::Outputs::Zabbix
