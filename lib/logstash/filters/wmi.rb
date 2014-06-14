# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This is a WMI query filter. It adds the executed wmi query result properties to the current event
class LogStash::Filters::Wmi < LogStash::Filters::Base

  config_name "wmi"
  milestone 2

  # The configuration for the wmi filter:
  #
  # Example:
  #
  #     input {
  #       wmi {
  #         query => "select * from Win32_Process"
  #       }
  #       wmi {
  #         query => "select PercentProcessorTime from Win32_PerfFormattedData_PerfOS_Processor where name = '_Total'"
  #       }
  #     }

  # Define the target field for placing the wmi query data. If this setting is
  # omitted, the WMI data will be stored at the root (top level) of the event.
  #
  # For example, if you want the data to be put in the 'doc' field:
  #
  #     filter {
  #       wsi {
  #         target => "doc"
  #       }
  #     }
  #
  #
  # NOTE: if the `target` field already exists, it will be overwritten!

  # WMI query
  config :query, :validate => :string, :required => true
  # optional target
  config :target, :validate => :string


  TIMESTAMP = "@timestamp"

  public
  def register

    @logger.info("Registering wmi filter", :query => @query)

    if RUBY_PLATFORM == "java"
      require "jruby-win32ole"
    else
      require "win32ole"
    end
  end # def register

  public
  def filter(event)
    return unless filter?(event)

	@wmi = WIN32OLE.connect("winmgmts://")

    @logger.debug("Running wmi filter", :event => event)

    if @target.nil?
      # Default is to write to the root of the event.
      dest = event.to_hash
    else
        dest = event[@target] ||= {}
    end

    begin
	  @query = event.sprintf(@query)
	  @logger.debug("Executing WMI query '#{@query}'")

	  @wmi.ExecQuery(@query).each do |wmiobj|
	      wmiobj.Properties_.each do |prop|
            dest[prop.name] = prop.value
          end
	  end

      filter_matched(event)
    rescue => e
      event.tag("_wmifailure")
      @logger.warn("Trouble executing wmi query", :exception => e)
      return
    end

    @logger.debug("Event after wmi filter", :event => event)

  end # def filter

end # class LogStash::Filters::Wmi
