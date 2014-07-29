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
    @threadsafe = false
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
       $firstEventHash[key] = { :key => key, :order => order, :event => event }
    elsif (( order == "last" ))
       @logger.debug(["merge", key, "Second Event for key", order])
       $secondEventHash[key] = { :key => key, :order => order, :event => event }
    end
    if $firstEventHash.include?(key) && $secondEventHash.include?(key) 
      time_delta = within_period(key) 
      if !time_delta.nil? then
        @logger.debug(["merge", key, "within period", $firstEventHash[key][:key], $secondEventHash[key][:key], $secondEventHash[key][:time_delta]])
        trigger(key)
        filter_matched(event)
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
  end

  private
  def trigger(key)
    
    firstEvent = $firstEventHash[key]
    secondEvent = $secondEventHash[key]
    @logger.debug(["merge", key, "trigger"])
    new_events = LogStash::Event.new
    new_events["source"] = Socket.gethostname
    new_events["merge.key"] = secondEvent[:key]
    new_events["merge.time_delta"] = secondEvent[:time_delta]
    firstEvent[:event].to_hash.each do |key, value|
      new_events[key] = value
    end
    secondEvent[:event].to_hash.each do |key, value|
      if new_events[key].nil? 
        new_events[key] = value
      elsif  (( ![ "@timestamp", "tags", "@version" ].include?(key) && new_events[key] != value )) 
        new_events["#{key}-2"] = value
      end
    end
    new_events["@timestamp"] = secondEvent[:event]["@timestamp"]
    @logger.debug(["merge",  firstEvent[:key], secondEvent[:key], "newEvent"])
    $firstEventHash.delete(key)
    $secondEventHash.delete(key)
    @merges << new_events
    new_events = nil
    
  end


  def within_period(key)
    firstEvent = $firstEventHash[key][:event]
    secondEvent = $secondEventHash[key][:event]
    time_delta = secondEvent["@timestamp"] - firstEvent["@timestamp"]
    @logger.debug(["merge", key, time_delta, "time_delta", firstEvent["@timestamp"], "first", secondEvent["@timestamp"], "second"])
    if time_delta >= 0 && time_delta <= @period then
      $firstEventHash[key].merge!({ :time_delta => time_delta })
      $secondEventHash[key].merge!({ :time_delta => time_delta })
      return true
    else
      return nil
    end
  end
end
