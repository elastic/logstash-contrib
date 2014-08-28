# encoding: utf-8

# LazyElapsed filter
#
# This filter is based on the elapsed filter. The difference is, it tracks a start event and multiple
# end events. It will continue to look for an end event until the time_to_wait is hit, at which point it 
# will take the last end event it saw. It generates a new event with the specifed tag_on_match tag as 
# well as an elapsed time in milliseconds in the elapsed_time field. 
#
# If the time_to_wait is hit without finding an end event, an error event is generated with the specified
# tag_on_error tag.
#
# If fields_to_copy_from_start or fields_to_copy_from_end are used, the specified fields will be copied 
# from either the start event or the end event to the generated event. If the generated event is an error, 
# only the fields from the start event are copied as there was no end event found. The fields are copied from
# the start event first and then from the end event. So, if the same field is specified by both
# fields_to_copy_from_start and fields_to_copy_from_end, the field will end up with the value from 
# the end event. 
#
# LazyElapsed supports some simple aggregation over the events it sees. The fields_to_record will group
# up the values of a field from all the events seen for a given unique id and will add it to the 
# generated event using the key of the hash value as the field name. fields_to_sum will do the same thing
# but will add up the values of the fields.
#
# The configuration looks like this:
#
# filter {
#   lazyelapsed {
#     start_tag => "event_start"
#     end_tag => "event_end"
#     tag_on_match => "matched_tag"
#     tag_on_error => "error_tag"
#     unique_id_field => "id field name"
#     timestamp_field => "@timestamp"
#     time_to_wait => seconds
#     fields_to_copy_from_start => ["some_field"]
#     fields_to_copy_from_end => ["another_field","another_field2"]
#     fields_to_record => {"my_fields" => "my_field"}
#     fields_to_sum => {"total_server_time" => "server_time"}
#   }
# }
#
# You can use a Grok filter to prepare the events for the lazyelapsed filter.
# An example of configuration can be:
#
#   filter {
#   grok {
#     match => ["message", "%{TIMESTAMP_ISO8601} START id: (?<task_id>.*)"]
#     add_tag => [ "event_start" ]
#   }
#
#   grok {
#     match => ["message", "%{TIMESTAMP_ISO8601} END id: (?<task_id>.*)"]
#     add_tag => [ "event_end"]
#   }
#
#   grok {
#     match => ["message", "%{TIMESTAMP_ISO8601} OTHEREND id: (?<task_id>.*)"]
#     add_tag => [ "event_end"]
#   }
#
##   lazyelapsed {
#     start_tag => "event_start"
#     end_tag => "event_end"
#     unique_id_field => "task_id"
#     timestamp_field => "@timestamp"
#     }
#   }
#

require "logstash/filters/base"
require "logstash/namespace"
require 'thread'

class LogStash::Filters::LazyElapsed < LogStash::Filters::Base
  config_name "lazyelapsed"
  milestone 1

  config :start_tag, :validate => :string, :required => true
  config :end_tag, :validate => :string, :required => true
  config :tag_on_match, :validate => :string, :required => false, :default => "lazy_elapsed_match"
  config :tag_on_error, :validate => :string, :required => false, :default => "lazy_elapsed_error"
  config :unique_id_field, :validate => :string, :required => true
  config :timestamp_field, :validate => :string, :required => true
  config :time_to_wait, :validate => :number, :required => false, :default => 1800
  config :fields_to_copy_from_start, :validate => :array, :required => false, :default => []
  config :fields_to_copy_from_end, :validate => :array, :required => false, :default => []
  config :fields_to_record, :validate => :hash, :required => false, :default => {} 
  config :fields_to_sum, :validate => :hash, :required => false, :default => {} 

  public
  def register
    @mutex = Mutex.new
    @start_events = {}
    @last_new_event = nil
    @logger.debug("LazyElapsed: registered")
  end

  # Accessors for testing
  def start_events
    @start_events
  end

  def last_new_event
    @last_new_event
  end

  # Respond to an event  
  def filter(event)
    return unless filter?(event)

    unique_id = event[@unique_id_field]
    return if unique_id.nil?

    if(start_event?(event))
      filter_matched(event)
      @logger.debug("LazyElapsed: Start event matched")
      @mutex.synchronize do
        unless(@start_events.has_key?(unique_id))
          new_element = LogStash::Filters::LazyElapsed::Element.new(event)
          @fields_to_sum.each_pair do |key, field|
            new_element.fields[key] = event[field] if event.include?(field)
          end
          @fields_to_record.each_pair do |key, field|
            if (event.include?(field))
              new_element.fields[key] = []
              new_element.fields[key] << event[field]
            end
          end
          @start_events[unique_id] = new_element
        end
      end

    elsif(end_event?(event))
      filter_matched(event)
      @logger.debug("LazyElapsed: End event matched")
      @mutex.synchronize do
        if (@start_events.has_key?(unique_id))
          element = @start_events[unique_id]
          element.end = event
          @fields_to_sum.each_pair do |key, field|
            element.fields[key] = element.fields[key] + event[field] if event.include?(field)
          end
          @fields_to_record.each_pair do |key, field|
            element.fields[key] << event[field] if event.include?(field)
          end
        else
          # End event with no start. Just ignore.
        end
      end
    end
  end 

  # The method is invoked by LogStash every 5 seconds.
  def flush
    events = []
    @mutex.synchronize do
      keys_to_delete = []
      @start_events.each_pair do |key, element|
        element.age += 5
        if (element.age >= @time_to_wait)
          @logger.debug("LazyElapsed: Time to wait expired for event.")

          if (element.end)
            # We are at the timeout and have an end element. Create the new event.
            @logger.debug("LazyElapsed: End event found. Generating elapsed event.")
            new_event = LogStash::Event.new
            new_event.tag(@tag_on_match)
            new_event[@unique_id_field] = element.start[@unique_id_field]

            # Support both a Time timestamp and a string for a timestamp.
            if element.start[@timestamp_field].is_a?(Time) 
              start_time = element.start[@timestamp_field]
            else
              begin 
                start_time = Time.parse(element.start[@timestamp_field])
              rescue
                start_time = Time.now # Set it to something?
              end
            end

            if element.end[@timestamp_field].is_a?(Time) 
              end_time = element.end[@timestamp_field]
            else
              begin 
                end_time = Time.parse(element.end[@timestamp_field])
              rescue
                end_time = start_time # Will result in an 0 if we can't parse end time.
              end
            end

            new_event["elapsed_time"] = ((end_time - start_time) * 1000).to_i

            new_event[@timestamp_field] = element.start[@timestamp_field]
            @fields_to_record.keys.each do |key|
              new_event[key] = element.fields[key] if element.fields.include?(key)
            end
            @fields_to_sum.keys.each do |key|
              new_event[key] = element.fields[key] if element.fields.include?(key)
            end
            @fields_to_copy_from_start.each do |key|
              new_event[key] = element.start[key] if element.start.include?(key)
            end
            @fields_to_copy_from_end.each do |key|
              new_event[key] = element.end[key] if element.end.include?(key)
            end
            @last_new_event = new_event
            events << new_event

          else
            # We reached the timeout without an end element. Create an error event.
            @logger.debug("LazyElapsed: End event not found. Generating error event.")
            error_event = LogStash::Event.new
            error_event.tag(@tag_on_error)
            error_event[@unique_id_field] = element.start[@unique_id_field]
            error_event[@timestamp_field] = element.start[@timestamp_field]
            @fields_to_copy_from_start.each do |key|
              error_event[key] = element.start[key] if element.start.include?(key)
            end
            @last_new_event = error_event
            events << error_event 
          end

          keys_to_delete << key
        end
      end 

      keys_to_delete.each do |key|
        @start_events.delete(key)
      end
    end
    return events
  end

  private
  def start_event?(event)
    return (event["tags"] != nil && event["tags"].include?(@start_tag))
  end

  def end_event?(event)
    return (event["tags"] != nil && event["tags"].include?(@end_tag))
  end
end 

class LogStash::Filters::LazyElapsed::Element
  attr_accessor :start, :end, :age, :fields

  def initialize(start)
    @start = start
    @end = nil
    @age = 0
    @fields = {}
  end
end
