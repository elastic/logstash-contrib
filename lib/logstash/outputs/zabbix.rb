# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "shellwords"
require "zabbixapi"

# The zabbix output is used for sending item data to zabbix via the
# zabbix_sender executable.
#
# For this output to work, your event must have the following fields:
#
# * "zabbix_host"    (the host configured in Zabbix)
# * "zabbix_item_key"    (the item key on the host in Zabbix)
# * "create_zabbix_item" (if true, then this plugin will try to create the zabbix item via the Zabbix API)
# * "zabbix_item_name"    (the name of the item to create if create_zabbix_item is specified)
# * "zabbix_item_description"    (the description of the item to create if create_zabbix_item is specified)
# * "zabbix_item_value_type"    (the value_type of the item to create if create_zabbix_item is specified)
#               Possible values:
#                       0 - numeric float;
#                       1 - character;
#                       2 - log;
#                       3 - numeric unsigned;
#                       4 - text.
# * "zabbix_item_data_type"    (the data_type of the item to create if create_zabbix_item is specified)
#               Possible values:
#                       0 - decimal;
#                       1 - octal;
#                       2 - hexadecimal;
#                       3 - boolean.
# * "zabbix_item_unit" (Item's unit if create_zabbix_item is specified)
# * "send_field"    (the field name that is sending to Zabbix)
#
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
#            "zabbix_item_key", "item.key",
#            "zabbix_item_name", "Item name",
#            "zabbix_item_description", "Item description",
#            "zabbix_item_value_type", 0,
#            "zabbix_item_data_type", 0,
#            "zabbix_item_unit", "record",
#            "send_field", "field_name"
#          ]
#       }
#        grok {
#          match => [ "message", "%{SYSLOGBASE} %{DATA:data}" ]
#          add_tag => [ "zabbix-sender" ]
#          add_field => [
#            "zabbix_host", "%{source_host}",
#            "zabbix_item_key", "item.key",
#            "zabbix_item_name", "Item name",
#            "zabbix_item_description", "Item description",
#            "zabbix_item_value_type", 0,
#            "zabbix_item_data_type", 0,
#            "zabbix_item_unit", "record",
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
#
#         # specify the zabbix api url
#         zabbix_url => "http://localhost/zabbix/api_jsonrpc.php"
#
#         # specify the zabbix api user
#          zabbix_user => "api"
#
#         # specify the zabbix api password
#          zabbix_pass => "password"
#
#         # specify the zabbix trapper host for item creation (default "")
#         zabbix_sender_host => "localhost"
#
#         # specify to use or not the zabbix_sender file (default false)
#         zabbix_user_sender_file => true
#
#         # specify the zabbix_sender trapper file path (default /tmp/zabbix_trapper_output)
#         zabbix_sender_file_path => "/tmp/zabbix_sender_
#
#         # specify if the zabbix sender file should containt the event @timestamp or not (default true
#         zabbix_sender_file_with_timestamp => true
#
#         # specify how often the zabbix_sender file will be send to zabbix server (in number of event, default 10)
#         zabbix_sender_flush_count => 100
#       }
#       }
#     }
class LogStash::Outputs::Zabbix < LogStash::Outputs::Base

  config_name "zabbix"
  milestone 3

  config :host, :validate => :string, :default => "localhost"
  config :port, :validate => :number, :default => 10051
  config :zabbix_sender, :validate => :path, :default => "/usr/local/bin/zabbix_sender"
  config :zabbix_url, :validate => :string, :default => "http://localhost/zabbix/api_jsonrpc.php"
  config :zabbix_user, :validate => :string, :default => "api"
  config :zabbix_pass, :validate => :string, :default => "password"
  config :zabbix_trapper_host, :validate => :string, :default => ""
  config :zabbix_use_sender_file, :validate => :boolean, :default => false
  config :zabbix_sender_file_path, :validate => :string, :default => "/tmp/zabbix_trapper_output"
  config :zabbix_sender_file_with_timestamp, :validate => :boolean, :default => true
  config :zabbix_sender_flush_count, :validate => :number, :default => 10

  public
  def register
    @logger.info("Connecting to zabbix API")
    # Init Zabbix API connexion
begin
      @zbx = ZabbixApi.connect(
        :url => @zabbix_url,
        :user => @zabbix_user,
        :password => @zabbix_pass
      )
    rescue => e
      @logger.warn("Error during Zabbix connexion",
                   :exception => e, :backtrace => e.backtrace)
    end
    @zbx_items = {}
    @zbx_hostids = {}
    @zbx_sender_event_count = 0
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

    item_keys = Array(event["zabbix_item_key"])
    if item_keys.empty?
      @logger.warn("Skipping zabbix output; zabbix_item field is missing",
                   :missed_event => event)
      return
    end

    create_item = event["zabbix_create_item"]

    item_names = Array(event["zabbix_item_name"])
    if create_item and item_names.empty?
      @logger.warn("Skipping zabbix output; zabbix_item_name is missing and zabbix_create_item is specified",
                     :missed_event => event)
        return
    end

    item_descriptions = Array(event["zabbix_item_description"])
    if create_item and item_descriptions.empty?
      @logger.warn("Skipping zabbix output; zabbix_item_description is missing and zabbix_create_item is specified",
                   :missed_event => event)
      return
    end

    item_value_types = Array(event["zabbix_item_value_type"])
    if create_item and item_value_types.empty?
      @logger.warn("Skipping zabbix output; zabbix_item_value_type is missing and zabbix_create_item is specified",
                   :missed_event => event)
      return
    end

    item_data_types = Array(event["zabbix_item_data_type"])
    if create_item and item_data_types.empty?
      @logger.warn("Skipping zabbix output; zabbix_item_data_type is missing and zabbix_create_item is specified",
                   :missed_event => event)
      return
    end

    item_units = Array(event["zabbix_item_unit"])
    if create_item and item_units.empty?
      @logger.warn("Skipping zabbix output; zabbix_item_unit is missing and zabbix_create_item is specified",
                   :missed_event => event)
      return
    end


    field = Array(event["send_field"])
    if field.empty?
      field = ["message"]
    end

    item_keys.each_with_index do |key, index|

      if field[index].nil? || (zmsg = event[field[index]]).nil?
        @logger.warn("No zabbix message to send in event field #{field[index].inspect}", :field => field, :index => index, :event => event)
        next
      end

      if create_item
        # retrieving zabbix host id
        if !@zbx_hostids.has_key?(host[index])
          @zbx_hostids[host[index]] = @zbx.hosts.get_id(:host => host[index])
        end

        # testing if item for host already exists
        if @zbx_items.has_key?(host[index]) and @zbx_items[host[index]].has_key?(item_keys[index])
          @logger.info("Item #{item_keys[index]} already exists in zabbix. skipping creation")
        else
          # Creating hash key for host
          @zbx_items[host[index]] ||= {}

          # Retrieving all items for host
          zbx_host_items = @zbx.query(:method => "item.get", :params => {"output"=> "extend",  "hostids" => @zbx_hostids[host[index]] })

          # indexing items by key
          zbx_host_items_by_key = {}
          zbx_host_items.each do |item|
            @logger.info("Found item : #{item["key_"]}")
            zbx_host_items_by_key[item["key_"]] = item
          end

          # storing item list in host items list
          @zbx_items[host[index]] = zbx_host_items_by_key

          if @zbx_items.has_key?(host[index]) and @zbx_items[host[index]].has_key?(item_keys[index])
            @logger.info("Item #{item_keys[index]} already exists in zabbix. skipping creation")
          else
            @logger.info("Creating zabbix item #{item_keys[index]}")
            begin
              @zbx.items.create(
                :name => item_names[index],
                :description => item_descriptions[index],
                :key_ => item_keys[index],
                :type => 2, #zabbix_trapper
                :value_type => item_value_types[index],
                :data_type => item_data_types[index],
                :formula => 1,
                :trapper_hosts => @zabbix_trapper_host,
                :delay => 0,
                :units => item_units[index],
                :hostid => @zbx_hostids[host[index]]
              )
            rescue => e
              @logger.warn("Error during item Zabbix item creation",
                           :item => item_keys[index],
                           :exception => e, :backtrace => e.backtrace)
            else
              # ensure that zabbix server do a configuration cache reload before sending data.
              # sleep(60)
            end
          end
        end
      end

      mode = "a"
      if @zabbix_use_sender_file
        if @zbx_sender_event_count == 0
          mode = "w+"
        end
        if @zbx_sender_event_count < @zabbix_sender_flush_count
          if @zabbix_sender_file_with_timestamp
            @logger.info("Printing data in zabbix_sender file #{@zabbix_sender_file_path} with timestamp #{event["@timestamp"].strftime('%s')}")   
            File.open(@zabbix_sender_file_path, mode) {
              |file| file.write("#{host[index]} #{item_keys[index]} #{event["@timestamp"].strftime('%s')} #{zmsg}\n")
            }
          else
            @logger.info("Printing data in zabbix_sender file #{@zabbix_sender_file_path} without timestamp")
            File.open(@zabbix_sender_file_path, mode) {
              |file| file.write("#{@host} #{item_keys[index]} #{zmsg}\n")
            }
          end
          @zbx_sender_event_count = @zbx_sender_event_count + 1
        else
          @logger.info("Reached #{@zabbix_sender_event_count}. sending data to zabbix")
          @zbx_sender_event_count = 0
          if @zabbix_sender_file_with_timestamp
            cmd = [@zabbix_sender, "-z", @host, "-p", @port, "-T", "-i", @zabbix_sender_file_path]
          else
            cmd = [@zabbix_sender, "-z", @host, "-p", @port, "-i", @zabbix_sender_file_path]
          end
        end
      else
        cmd = [@zabbix_sender, "-z", @host, "-p", @port, "-s", host[index].to_s, "-k", item_keys[index].to_s, "-o", zmsg.to_s, "-v"]
      end

      if cmd
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
        end # begin popen

      end # id cmd
    end # item.each_with_index
  end # def receive
end # class LogStash::Outputs::Zabbix
