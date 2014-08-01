# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# INFORMATION:
# The filter Chainable was designed for detect which events are chained from another events
# Some events are generated due a subsequent event. This plugin can mark those events as chained
# 
# This plugin is very useful for industrial applications logs, so if for example one photocell from 
# a certain area fails, then the Chainable plugin can mark all the events that has been generated
# in the same section and area during a specific number of seconds. This plugin can be used for detect
# repeated events as well.
#
# The Chainable filter will add the field @chained with the values true or false, so it is possible filter the events very easily with Kibana.

# INFORMATION ABOUT CLASS:
 
# For do this job, the plugin save a copy of those events in the cache during time_adv and after that
# confront the new events with the cached ones.

# USAGE:

# This is an example of logstash config:

# filter{
#  chainable {
#     time_adv => 15                    
#     fields   => ["deviceIdentifier", "errorCode"]
#  }
# }

# We analize this:

# time_adv => 1
# Means the time that events are saved in the cache.
# If for example time_adv is equal to 10, it means that new events are confronted against the events that has been generated in the last 10 seconds
# After 10 seconds the events are deleted from the cache and free the memory used by them.

# fields => ["deviceIdentifier", "errorCode"]
# Means the fields that you want compare, if one comparation fails against one field, then the @chainable is equal to false

class LogStash::Filters::Chainable < LogStash::Filters::Base

 config_name "chainable"
 milestone 1

# If you do not set time_adv the plugin does nothing.
 config :time_adv, :validate => :number, :default => 0
 
# Fields to compare/confront
config :fields, :validate => :array, :default => []
 
 public
 def register

  # Control the correct config
  if (!(@time_adv == 0) || !(@fields.empty))
    # Is used for store the different events.
    @sarray = Array.new
  
  else
   @logger.warn("Chainable: you have not specified Time_adv and fields. This filter will do nothing!")
  end

 end

# Remove those events that were created time_adv before
protected
 def removeOldEvents

 @sarray.delete_if {|event| event["_ctimestamp"] < Time.now.getutc - time_adv}
 
 # File.open("debug.log", 'a') { |file| file.write(@sarray.to_yaml) }
 
end

# Comparate fields
protected
 def compareFields(event, cevent)

  matched = false

  @fields.each do |field|    
    if (event[field] == cevent[field])
      matched = true      
    else      
      matched = false      
      break
    end
  end 

  return matched
end


 public
 def filter(event)
  return unless filter?(event)
                                     
  @chained = false;

  # Control the correct config
  if(!(@time_adv == 0))

    @message = event.clone;

    event["@chained"] = false;
    
    # Remove the old data from the cache
    removeOldEvents();    

    # control if the events are new or they came before
    @sarray.each do |cevent|      
      if (compareFields(event, cevent))
        event["@chained"] = true;
        @logger.debug("Chainable: Event match")
        break
      end
    end 
    
    @message["_ctimestamp"] = Time.now.getutc
    @sarray << @message    
      
  else
   @logger.warn("Chainable: you have not specified Time_adv. This filter will do nothing!")
  end
  
 end

end
