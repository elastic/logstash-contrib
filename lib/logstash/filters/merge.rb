# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


class LogStash::Filters::Merge < LogStash::Filters::Base
  config_name "merge"
  milestone 1

  config :key, :validate => :array

  config :period, :validate => :number, :default => 60
  
  config :order, :validate => :number, :default => nil

  config :heartbeat, :validate => :boolean, :default => false

  config :event_on_expire, :validate => :boolean, :default => false

  config :merge_tag, :validate => :array, :default => nil

  config :fields_to_merge, :validate => :array, :default => nil

  config :event_count, :validate => :number, :default => 2

  config :new_event, :validate => :boolean, :default => false


  def register
    @threadsafe = false
    @logger.debug("registering")
    @merges = []
  end


  def filter(event)
    return unless filter?(event)
      @now = Time.now
      order = event.sprintf(@order)
      if $optHash.nil? then
        @logger.debug(["merge", "New option hash"])
        $optHash = Hash.new
      end
      if $eventHash.nil? then
        @logger.debug(["merge", "New event hash"])
        $eventHash = Hash.new
      end
      if $expireKeyHash.nil? then
        @logger.debug(["merge", "New expire hash"])
        $expireKeyHash = Hash.new
      end
      @filter_match=true
      begin
        if @heartbeat
          @logger.debug(["merge", key,"Heartbeat - Check if Expired"])
          expireMerges(@now)
          return 
        end
        @logger.debug(["merge", @key,"Key"])
        saveEvent(@key, event)
        checkKeys(@key, event)
      rescue
        @logger.error(["merge", @key,"Error!"])
      end
     
  end


  def flush
    return if @merges.empty?
    @logger.debug(["merge", "Flushing", @merges])
    new_events = @merges
    @merges =[]
    new_events
  end

  private
  def saveEvent(key, event)
     return unless key
     key.each do |key|
       key = event.sprintf(key)
       period = @period.to_i
       $eventHash["#{key}-#{order}"] = { :key => key, :order => order, :event => event, :period => @period , :merge_tag => @merge_tag, :fields_to_merge => @fields_to_merge }
       if order == 1
         @logger.debug(["merge", "saving options"])
         $optHash[key] = { :key => key, :period => @period , :event_count => @event_count, :new_event => @new_event }
       end
       if @event_on_expire
         expiration = @now + period
         $expireKeyHash[key] = { :expiration => expiration }
       end
       filter_matched(event)
    end
  end


  def checkKeys(key, event) 
     return unless key
     key.each do |key|
       key = event.sprintf(key)
       missing = [] 
       if $optHash.include?(key)
         options = $optHash[key]
         @logger.debug(["merge", "options", options])
       else
         @logger.debug(["merge", key, "Missing options!"])
         return
       end
       totalEvents=1.step(options[:event_count]).to_a
       @logger.debug(["merge", key, totalEvents, "total events"])
       
       totalEvents.each do |num|
         if !$eventHash.include?("#{key}-#{num}")
           (missing ||= []) << num
         end
       end
       #missing something quit
       if missing.any?
         @logger.debug(["merge", key, missing, "missing an event"])
         return
       end     
       time_delta = within_period(key, options) 
       if !time_delta.nil? then
         @logger.debug(["merge", key, "within period", key ])
         if options[:new_event]    
           mergeToNewEvent(key, options)
         else
           updateEvent(key, event, options)
         end
       else
         @logger.debug(["merge", key, "ignoring (not in period)"])
       end
    #    rescue
    #      @logger.error(["merge", "Error has occured with", key])
    #    end
    end
  end


  private 
  def mergeEvents(sEvent, dEvent, fields_to_merge)
    @logger.debug(["merge", key, "fields to merge", fields_to_merge])
    sEvent.to_hash.each do |key, value|
      if !fields_to_merge.any? || fields_to_merge.include?(key)
        if dEvent[key].nil? 
          dEvent[key] = value
        elsif  (( ![ "@timestamp", "tags", "@version" ].include?(key) && dEvent[key] != value )) 
          dEvent["#{key}-2"] = value
        elsif (( key == "tags" ))
          value.each do |tag|
            (dEvent["tags"] ||= []) << tag
          end
        end
      end
    end
    @logger.debug(["merge", "hash", dEvent["tags"]])
    return dEvent
  end


  private
  def mergeToNewEvent(key, options)
    @logger.debug(["merge", key, "mergeToNewEvent"])
    totalEvents=1.step(options[:event_count]).to_a
    new_events = LogStash::Event.new
    new_events["source"] = Socket.gethostname
    new_events["merge.key"] = key
    new_events["merge.time_delta"] = $eventHash["#{key}-1"][:time_delta]
    totalEvents.each do |num|
      new_events = mergeEvents($eventHash["#{key}-#{num}"][:event], new_events, $eventHash["#{key}-#{num}"][:fields_to_merge])
      @logger.debug(["merged", key, num ])
      $eventHash["#{key}-#{num}"][:merge_tag].each do |merge_tag|
        (new_events["tags"] ||= []) << merge_tag
      end
      $eventHash.delete("#{key}-#{num}")
    end
    $expireKeyHash.delete(key)
    @merges << new_events
    new_events = nil
  end

  private
  def updateEvent(key, event, options)
    totalEvents=1.step(options[:event_count]).to_a
    @logger.debug(["merge", key, "mergeToNewEvent"])
    event["merge.key"] = options[:key]
    event["merge.time_delta"] = $eventHash["#{key}-1"][:time_delta]
    totalEvents.each do |num|
      if num != @order
        event = mergeEvents($eventHash["#{key}-#{num}"][:event], event, $eventHash["#{key}-#{num}"][:fields_to_merge])
        @logger.debug(["merged", key, num ])
      end
      $eventHash["#{key}-#{num}"][:merge_tag].each do |merge_tag|
        (event["tags"] ||= []) << merge_tag
      end
      $eventHash.delete("#{key}-#{num}")
    end
    @logger.debug(["merge", "hash", event["tags"]])
    $expireKeyHash.delete(key)
    @logger.debug(["merge", "newTags", event["tags"]])
    
  end


  private
  def expireMerges(now)
    @next_expiration = nil
    return unless !$expireKeyHash.nil?
    $expireKeyHash.each do |key, field|
      expiration = field[:expiration]
      @logger.debug(["merge", "Check if expired", key, expiration, now])
      expired = expiration <= @now
      if expired then
        if $optHash.include?(key)
          options = $optHash[key]
        else
          @logger.error(["merge", key, "Missing options, can't expire!"])
          return
        end
        totalEvents=1.step(options[:event_count]).to_a
        new_events = LogStash::Event.new
        new_events["source"] = Socket.gethostname
        new_events["merge.expiration"] = expiration
        new_events["merge.key"] = key
        totalEvents.each do |num|
  #        begin
          if $eventHash.include?("#{key}-#{num}")  
            new_events = mergeEvents($eventHash["#{key}-#{num}"][:event], new_events, $eventHash["#{key}-#{num}"][:fields_to_merge])
            @logger.debug(["merged", key, num ])
            $eventHash["#{key}-#{num}"][:merge_tag].each do |merge_tag|
              (new_events["tags"] ||= []) << merge_tag
            end
            $eventHash.delete("#{key}-#{num}")
          else
            (new_events["merge.missing_events"] ||= []) << num
            @logger.debug(["merge", key, num, "Missing Event"])
          end   
        end
        @logger.debug(["merge", "Expiring captured events", key])
        (new_events["tags"] ||= []) << "merge.expired"
        @merges << new_events
        new_events = nil
        $expireKeyHash.delete(key)
      end
    end
  end 



  def within_period(key, options)
    event_count = options[:event_count]
    firstEvent = $eventHash["#{key}-1"][:event]
    lastEvent = $eventHash["#{key}-#{event_count}"][:event] 
    period = options[:period]
    time_delta = lastEvent["@timestamp"] - firstEvent["@timestamp"]
    @logger.debug(["merge", key, time_delta, "time_delta", firstEvent["@timestamp"], "first", lastEvent["@timestamp"], "second"])
    if time_delta.abs <= period then
      $eventHash["#{key}-1"].merge!({ :time_delta => time_delta })
      return true
    else
      return nil
    end
  end
end
