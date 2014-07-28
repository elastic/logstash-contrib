# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


class LogStash::Filters::Merge < LogStash::Filters::Base
  config_name "merge"
  milestone 1

  #key - Set same on pair of events to track/merge
  config :key, :validate => :string, :required => true
  # syntax: `period => 60`
  config :period, :validate => :number, :default => 5
  #Order events should be, 'first' || 'last' 
  config :order, :validate => :string, :default => nil, :required => true


  def register
    @logger.debug("registering")
    @merges = []
  end


  def filter(event)
    return unless filter?(event)
    key = event.sprintf(@key)
    order = event.sprintf(@order)
    if $firstEventHash.nil? then
      @logger.debug(["merge", "New KeyList"])
      $firstEventHash = Hash.new
    end
    if $secondEventHash.nil? then
      @logger.debug(["merge", "New KeyList"])
      $secondEventHash = Hash.new
    end

    if (( order == "first" )) 
       @logger.debug(["merge", key, "First Event for key", order ])
       $firstEventHash[key] = event
    elsif (( order == "last" ))
       @logger.debug(["merge", key, "Second Event for key", order])
      $secondEventHash[key] = event
    end
    if $firstEventHash.include?(key) && $secondEventHash.include?(key) 
      time_delta = within_period(key) 
      if !time_delta.nil? then
        @logger.debug(["merge", key, "within period"])
        trigger(key, time_delta)
      else
        @logger.debug(["merge", key, "ignoring (not in period)"])
      end
    end
  end

  def flush
    return if @merges.empty?
    @logger.debug(["merge", "Flushing"])
    new_events = @merges
    @merges =[]
    new_events

#    return [new_events]
  end

  private

  def first_event(event)
    @logger.debug(["merge", key, "start_period"])
    $keyList[key] = event
  end

  def trigger(key, time_delta)
    @logger.debug(["merge", key, "trigger"])
    new_events = LogStash::Event.new
    new_events["source"] = Socket.gethostname
    new_events["tags"] = [@add_tag]
    new_events["merge.time_delta"] = time_delta
    new_events.append($firstEventHash[key])
    new_events.append($secondEventHash[key])
    new_events["@timestamp"] = $firstEventHash[key]["@timestamp"]
    @logger.debug(["merge", key, "newEvent"])
    $firstEventHash.delete(key)
    $secondEventHash.delete(key)
    @merges << new_events
    
  end

  def followed_by_tags_match(event)
    @logger.debug(["merge", key, "tags", event["tags"], @followed_by_tags])
    (event["tags"] & @followed_by_tags).size == @followed_by_tags.size
  end

  def within_period(key)
    firstEvent = $firstEventHash[key]
    secondEvent = $secondEventHash[key]
    time_delta = secondEvent["@timestamp"] - firstEvent["@timestamp"]
    @logger.debug(["merge", key, time_delta, "time_delta", firstEvent["@timestamp"], "first", secondEvent["@timestamp"], "second"])
    if time_delta >= 0 && time_delta <= @period then
      return time_delta
    else
      return nil
    end
  end
end
