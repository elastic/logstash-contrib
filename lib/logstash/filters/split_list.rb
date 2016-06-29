# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# The split_list filter is for splitting a list of items into new events.
# A clone will be made for each item in the list stored in the target field.
# The original event is left unchanged.
class LogStash::Filters::SplitList < LogStash::Filters::Base

  config_name "split_list"
  milestone 1

  # A new event will be created for each item in this field
  config :source, :validate => :string, :default => "message"

  # Define the target field for placing the data in the new event. If this
  # setting is omitted, the item will be stored at the root (top level) of the
  # event.
  config :target, :validate => :string

  # Set type of new event - default is matching the original event
  config :new_type, :validate => :string

  # Remove the original key on new events unless this is true
  config :keep_source, :validate => :boolean, :default => false

  public
  def register
    # Nothing to do
  end

  public
  def filter(event)
    return unless filter?(event)

    @logger.debug("Splitting event", :event => event, :source => @source)

    event[@source].each do |item|
      clone = event.clone

      # Remove original key from clone
      clone.remove(@source) unless @keep_source

      if @target.nil?
        # Merge into root of event - item must be an object/hash for this to work
        dest = clone.to_hash
        dest.merge!(item)
      else
        # Overwrite field at :target - item can be any data type
        dest = clone[@target] = item
      end

      if not @new_type.nil?
        clone["type"] = @new_type
      end

      @logger.debug("Split new event", :new => clone, :event => event)

      filter_matched(clone)

      # Push this new event onto the stack at the LogStash::FilterWorker
      yield clone
    end
  end

end # class LogStash::Filters::SplitList
