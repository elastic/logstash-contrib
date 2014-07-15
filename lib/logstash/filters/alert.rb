require "logstash/filters/base"
require "logstash/namespace"

#
# The Alert filter is for alerting on either passing a Max threshold in a given time or
# failing to meet a min threshold in a period of time. There is a 'heartbeat'  that when
# combined with the min threshold allows one to watch for a particular event and alert
# if it does not occur in the expected time period. Whenever a threshold is met a single
# event is generated. Allowing one to generate an alert/email/whatever without fear of 
# spamming one self.
#
# For example, if you wanted to alert if you see more than 10 404s in one hour, your config may
# look like:
#  if [response] == "404" {
#    alert {
#    max_threshold => 10
#    max_threshold_msg => '404 Alert'
#    period => 3600
#    key => "404"
#    filter_tag => "404 Alarm"
#    }
#
# Which would result in(1 sec between events):
#     event 1 - counted, no event created
#     event 2 - counted, no event created
#     event 3 - counted, no event created
#     event 4 - counted, no event created
#     event 5 - counted, no event created
#     event 6 - counted, no event created
#     event 7 - counted, no event created
#     event 8 - counted, no event created
#     event 9 - counted, no event created
#     event 10 - counted, no event created
#     event 11 - counted, threshold passed, event created
#     event 12 - counted, threshold passed, event already created -- no new event
#     ...
# 
# A common use case would be to alert if you see more then X errors in a time period. i.e. More than 10 "404" errors in a 5 minute period. Here is a conf to parase httpd access logs and generate a single email if more than 10 404s happen in a 5 minute period.
#
# input {
#
#  file {
#    path => "/var/log/httpd/access_log"
#    type => "httpd"
#  }
# } 
#
#
# filter {
#  grok {
#    match => ["message", "%{COMBINEDAPACHELOG}" ]
#  }
#  date {
#  #14/Jun/2014:23:31:16 -0400
#    match => ["timestamp", "dd/MMM/YYYY:HH:mm:ss Z"]
#  }
# 
#  if [response] == "404" {
#    alert {
#    max_threshold => 10
#    max_threshold_msg => '404 Alert'
#    period => 300
#    key => "404-Alert"
#    }
#
# output {
#    if "alert_filter" in [tags] {
#      if [message] == "404 Alert"  {
#        email {
#           from => "logstash@dopey.io"
#           subject => "%{message}"
#           to => "dopey@dopey.io"
#           via => "sendmail"
#           body => "Max Threshold:%{max_threshold}\n\nNum of 404s:%{alert_filter.count}"
#           options => { "location" => "/sbin/sendmail" }
#        }
#      }
#   }
# }
# Another case would be to alert if a min threshold is *not* met. If you have an ongoing process with an expected volume of output, you can alert if this process stops producing the expected volume in a given time. Continuing the httpd theme, here is an alert if your site does not have 10 202s in an hour period. i.e. no activity. 
#
#  if [response] == "202" {
#    alert {
#    min_threshold => 10
#    min_threshold_msg => "202s Not Happenin"
#    period => 3600
#    key => "202"
#    }
#  }
#
# You can add the 'heartbeat' flag as a companion to the min_threshold alert. The above alert would only alert when the next 202 event happened after the expiration time -- Since that is the only time alert filter would be called.  This may be fine in many situations. However, if you want an alert even if the above filter is never matched you can add an alert filter by itself with the heartbeat flag. This will be ran on every event. For now you need to mirror your above configuration into this stanza. This will lookup the key below, evaluate the current count vs expiration time then exit. 
#
#  alert {
#  heartbeat => true
#  min_threshold => 10
#  min_threshold_msg => "202s Not Happenin"
#  period => 3600
#  key => "202"
#  }
#
# output {
#    if "alert_filter" in [tags] {
#      if [message] == "202s Not Happenin"  {
#        email {
#           from => "logstash@dopey.io"
#           subject => "%{message}"
#           to => "dopey@dopey.io"
#           via => "sendmail"
#           body => "Min 202s Expected:%{min_threshold}\n\nNum of 202s:%{alert_filter.count}"
#           options => { "location" => "/sbin/sendmail" }
#        }
#      }
#   }
# }


#
# The event counts are cleared after the configured period elapses since the 
# first instance of the event. That is, all the counts don't reset at the same 
# time but rather the expiration period is per unique key value.
#
# Jacob Morgan (@jpyth - http://dopey.io )
# 
class LogStash::Filters::Alert < LogStash::Filters::Base

  # The name to use in configuration files.
  config_name "alert"

  # New plugins should start life at milestone 1.
  milestone 1

  # The key used to identify events. Events with the same key will be counted/alerted as a group.
  config :key, :validate => :string,  :required => true
  
  #Tag to create when generating the alert event.
  config :filter_tag, :validate => :string, :default => "alert_filter", :required => false

  # If less events than this threshold are seen during set period a single alert will be generated.
  config :min_threshold, :validate => :number, :default => -1, :required => false
  config :min_threshold_msg, :validate => :string, :default => "Min Threshold Alert", :required => false
  

  # If more events than this threshold are seen during set period a single alert will be generated.
  config :max_threshold, :validate => :number, :default => -1, :required => false
  config :max_threshold_msg, :validate => :string, :default => "Max Threshold Alert", :required => false

  # heartbeat flag -- Use with min_threshold to monitor/alert on lack of events
  config :heartbeat, :validate => :boolean, :default => false

  # The period in seconds after the first occurrence of an event until the count is 
  # reset for the event. This period is tracked per unique key value.  Field
  # substitutions are allowed in this value.  
  config :period, :validate => :string, :required => true
  
  # The maximum number of counters to store before the oldest counter is purged. Setting 
  # this value to -1 will prevent an upper bound no constraint on the number of counters  
  # and they will only be purged after expiration. This configuration value should only 
  # be used as a memory control mechanism and can cause early counter expiration if the 
  # value is reached. It is recommended to leave the default value and ensure that your 
  # key is selected such that it limits the number of counters required (i.e. don't 
  # use UUID as the key!)
  config :max_counters, :validate => :number, :default => 100000, :required => false

  # Performs initialization of the filter.
  public
  def register
    @threadsafe = false
 #   $event_counters = Hash.new
    @next_expiration = nil
  end # def register

  # Filters the event. The filter is successful if the event should be throttled.
  public
  def filter(event)
      	  
    # Return nothing unless there's an actual filter event
    return unless filter?(event)
    	  
    @now = Time.now
    #use key from conf
    key = @key    
    @logger.debug? and @logger.debug("filters/#{self.class.name}: Key", 
      { :key => key })

    #If we don't have an event_counter, make one.
    if $event_counters.nil? then
      @logger.debug? and @logger.debug("filters/#{self.class.name}: New event counter hash") 
      $event_counters = Hash.new
    end
    #If we don't have an alert Array create it. Stores alerts to be created into events.
    if $alerts.nil? then
      @logger.debug? and @logger.debug("filters/#{self.class.name}: New alert array") 
      $alerts = Array.new
    end

    # Purge counters if too large to prevent OOM.
    if @max_counters != -1 && $event_counters.size > @max_counters then
      purgeOldestEventCounter()
    end
    
    counter = nil
    #if event counter does not exist, create it
    if (( !$event_counters.include?(key) )) then
      period = @period.to_i      
      expiration = @now + period
      $event_counters[@key] = { :key => @key, :count => 0, :expiration => expiration, :min_threshold => min_threshold, 
        :min_threshold_msg => min_threshold_msg, :max_threshold => max_threshold, :max_threshold_msg => max_threshold_msg,  
        :period => period, :filter_tag => filter_tag, :alerted => false }
      counter = $event_counters[@key]
      @alerted=false 
      @logger.debug? and @logger.debug("filters/#{self.class.name}: new event key created", 
      	  { :key => counter})
      #If this is the heartbeat cycle, exit Nothing more to do.
      if (( @heartbeat == true )) then
        return 
      end 
    #If it already exist, get it
    elsif (($event_counters.include?(key))) then 
      counter = $event_counters[@key]
      @logger.debug? and @logger.debug("filters/#{self.class.name}: key found", 
      	  { :heartbeat => @heartbeat, :key => @key, :array => counter })
    end

    # Expire existing counter if needed
    if @next_expiration.nil? || @now >= @next_expiration || @now >= counter[:expiration] then
        if (( @heartbeat == true )) then
    	  expireEventCounters(@now)
        @logger.debug? and @logger.debug(
      	  "filters/#{self.class.name}: heartbeat", 
      	  { "next_expiration" => @next_expiration, "heartbeat expire counters" => @heartbeat })
          return
        end
    	expireEventCounters(@now)
        @logger.debug? and @logger.debug(
      	  "filters/#{self.class.name}: expire counters regular", 
      	  { "next_expiration" => @next_expiration })
    end 

    #if heartbeat return, nothing more to do this cycle.
    if (( @heartbeat == true )) then
      return
    end
    

    
    @logger.debug? and @logger.debug(
      	  "filters/#{self.class.name}: next expiration", 
      	  { "next_expiration" => @next_expiration, "array" => counter })
    

    # Count this event
    counter[:count] = counter[:count] + 1;
    @logger.debug? and @logger.debug("filters/#{self.class.name}: current count", 
      	  { :key => key, :array => counter})
    
    # Alert if count is  > max threshold
    if ((counter[:max_threshold] != -1 )) &&  ((counter[:count] > counter[:max_threshold])) && 
       ((counter[:alerted] == false )) && ((@heartbeat == false ))  then
      #save to alert array
      $alerts <<  counter
      counter[:alerted] = true
      @logger.debug? and @logger.debug(
      	  "filters/#{self.class.name}: Alerting on event max threshold", { :alerted => $alerted, :alerts => $alerts })
    end
        
  end # def filter




 #Send event if either threshold is met.  
  def flush
  
    return unless should_flush? 
    alertArray = $alerts.shift
    if (( alertArray.nil? )) || (( alertArray[:eventCreated] == true )) then
      return
    end
    #Attempt to prevent double events -- Dirty hack.
    alertArray[:eventCreated] = true
    @logger.debug? and @logger.debug(
      "filters/#{self.class.name}: All Alerts in array ", { :alertsArray => $alerts })
      @logger.debug? and @logger.debug(
        "filters/#{self.class.name}: Alert being processed array ", { :counterArray => alertArray })
      @filter_tag = alertArray[:filter_tag]
      event = LogStash::Event.new
      if (( alertArray[:max_threshold] != -1 )) then
        event["max_threshold"] =  alertArray[:max_threshold]
        event["message"] =  alertArray[:max_threshold_msg]
      elsif (( alertArray[:min_threshold] != -1 )) then
        event["min_threshold"] =  alertArray[:min_threshold]
        event["message"] =  alertArray[:min_threshold_msg]
      end
      event["#{@filter_tag}.key"] = alertArray[:key]
      event["#{@filter_tag}.count"] = alertArray[:count]
      event["#{@filter_tag}.expiration"] = alertArray[:expiration]
      event["#{@filter_tag}.period"] = alertArray[:period]
      event.tag alertArray[:filter_tag]
    
      @logger.debug? and @logger.debug(
        "filters/#{self.class.name}: Alert", { :event => event })
      return [event]
  end #flush
  
  #to flush or not -- check if alert array is nil
  def should_flush?
    if (( !$alerts.nil? ))  then
      @logger.debug? and @logger.debug(
        "filters/#{self.class.name}: Should flush ")
      return true
    else
      @logger.debug? and @logger.debug(
      "filters/#{self.class.name}: Should not flush")
      return false
    end
  end


  private
  def expireEventCounters(now) 
    
    @next_expiration = nil
    $event_counters.each_pair do |key, counter|
      min_threshold = counter[:min_threshold]
      expiration = counter[:expiration]
      expired = expiration <= @now
      @logger.debug? and @logger.debug(
      "filters/#{self.class.name}: expire event", { :counter => counter[:count], :min => min_threshold})
    
      if expired then
        #If under min threshold, store to alert array
        if ((counter[:count] < min_threshold)) then 
          $alerts <<  counter
          @logger.debug? and @logger.debug(
      	  "filters/#{self.class.name}: Alerting on event min threshold", { :alerts => $alerts })
        else
          @logger.debug? and @logger.debug(
            "filters/#{self.class.name}: Expired Above Min Threshold- No Alert", 
             { :heartbeat => @heartbeat, :key => @key, :count => counter[:count], :min => min_threshold })
        end
        #expired event, delete it. 
        $event_counters.delete(key)
      elsif @next_expiration.nil? || (expiration < @next_expiration)
        @next_expiration = expiration
        @logger.debug? and @logger.debug(
        "filters/#{self.class.name}: Setting expiration", { :expiration => expiration, :next => @next_expiration })
      end
    end
  
  end # def expireEventCounters
  
  # Purges the oldest event counter. This operation is for memory control only 
  # and can cause early period expiration and thrashing if invoked.
  private
  def purgeOldestEventCounter()
    
    # Return unless we have something to purge
    return unless $event_counters.size > 0
    
    oldestCounter = nil
    oldestKey = nil
    
    $event_counters.each do |key, counter|
      if oldestCounter.nil? || counter[:expiration] < oldestCounter[:expiration] then
        oldestKey = key;
        oldestCounter = counter;
      end
    end
    
    @logger.warn? and @logger.warn(
      "filters/#{self.class.name}: Purging oldest counter because max_counters " +
      "exceeded. Use a better key to prevent too many unique event counters.", 
      { :key => oldestKey, :expiration => oldestCounter[:expiration] })
      	  
    $event_counters.delete(oldestKey)
    
  end
end # class LogStash::Filters::Alert
