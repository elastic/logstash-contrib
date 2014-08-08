# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


class LogStash::Filters::Merge < LogStash::Filters::Base
  config_name "merge"
  milestone 1

  #key - Set same on pair of events to track/merge
  config :key, :validate => :array, :required => true
  # syntax: `period => 60`
  config :period, :validate => :number, :default => 60
  #Order events should be, 'first' || 'last' 
  config :order, :validate => :string, :default => nil, :required => true

  config :merge_tag, :validate => :array, :default => nil

  def register
    @threadsafe = false
    @logger.debug("registering")
    @merges = []
  end


  def filter(event)
    return unless filter?(event)
    begin 
      order = event.sprintf(@order)
      if $firstEventHash.nil? then
        @logger.debug(["merge", "New KeyList"])
        $firstEventHash = Hash.new
      end
      if $secondEventHash.nil? then
        @logger.debug(["merge", "New KeyList"])
        $secondEventHash = Hash.new
      end
      @filter_match=true
      @key.each do |key|
        key = event.sprintf(key)
        @logger.debug(["merge", key, "key loop"])
        if (( order == "first" )) 
          @logger.debug(["merge", key, "First Event for key", order ])
          $firstEventHash[key] = { :key => key, :order => order, :event => event, :period => @period , :merge_tag => @merge_tag}
        elsif (( order == "last" ))
          @logger.debug(["merge", key, "Second Event for key", order])
          $secondEventHash[key] = { :key => key, :order => order, :event => event, :merge_tag => @merge_tag}
        else
          @logger.debug(["merge", key, order, "If you see this, your config is wrong. -- 'order'"])
          @filter_match = nil
        end
        if !@filter_match.nil? 
          @logger.debug(["merge", key, @filter_match, order, "filter_matched"])
          filter_matched(event)
          @filter_match=nil
        end
        if $firstEventHash.include?(key) && $secondEventHash.include?(key) 
          time_delta = within_period(key) 
          if !time_delta.nil? then
            @logger.debug(["merge", key, "within period", $firstEventHash[key][:key], $secondEventHash[key][:key], $secondEventHash[key][:time_delta]])
            trigger(key)
          else
            @logger.debug(["merge", key, "ignoring (not in period)"])
          end
        end
      end
    rescue
      @logger.error(["merge", "Error has occured with", key])
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
    new_events["merge.key"] = firstEvent[:key]
    new_events["merge.time_delta"] = firstEvent[:time_delta]
    @logger.debug(["merge", "Tags", firstEvent[:event]["tags"]])
    @logger.debug(["merge", "Tags", secondEvent[:event]["tags"]])
    firstEvent[:event].to_hash.each do |key, value|
      new_events[key] = value
    end
    secondEvent[:event].to_hash.each do |key, value|
      if new_events[key].nil? 
        new_events[key] = value
      elsif  (( ![ "@timestamp", "tags", "@version" ].include?(key) && new_events[key] != value )) 
        new_events["#{key}-2"] = value
      elsif (( key == "tags" ))
        value.each do |tag|
          (new_events["tags"] |= []) << tag
        end
      end
    end
    @logger.debug(["merge", "hash", new_events["tags"]])
    #Add Merge tags
    merge_tags= firstEvent[:merge_tag]|secondEvent[:merge_tag]
    merge_tags.each do |merge_tag|
      (new_events["tags"] ||= []) << merge_tag
    end
    new_events["@timestamp"] = secondEvent[:event]["@timestamp"]
    @logger.debug(["merge",  firstEvent[:key], secondEvent[:key], "newEvent"])
    $firstEventHash.delete(key)
    $secondEventHash.delete(key)
    @logger.debug(["merge", "newTags", new_events["tags"]])
    @merges << new_events
    new_events = nil
    
  end


  def within_period(key)
    firstEvent = $firstEventHash[key][:event]
    secondEvent = $secondEventHash[key][:event] 
    period = $firstEventHash[key][:period]
    time_delta = secondEvent["@timestamp"] - firstEvent["@timestamp"]
    @logger.debug(["merge", key, time_delta, "time_delta", firstEvent["@timestamp"], "first", secondEvent["@timestamp"], "second"])
    if time_delta >= 0 && time_delta <= period then
      $firstEventHash[key].merge!({ :time_delta => time_delta })
      return true
    else
      return nil
    end
  end
end
