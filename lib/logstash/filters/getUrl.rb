# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'net/http'
require 'json'


class LogStash::Filters::GetUrl < LogStash::Filters::Base
  config_name "getUrl"
  milestone 1

  config :url, :validate => :string, :required => true
  #in milliseconds 
  config :timeout, :validate => :number, :default => 50
  
  config :fields_to_merge, :validate => :array, :default => nil



  def register
    @threadsafe = true
    @logger.debug("registering")
  end


  def filter(event)
    return unless filter?(event)
    begin 
      url=event.sprintf(@url)
      json=getUrl(url, @timeout)    
      @logger.debug(["getUrl", json, timeout])
      if json
        addToEvent(event, json, @fields_to_merge)
        filter_matched(event)
      end
    rescue
      @logger.error(["getUrl", "Something went horribly wrong!"])
    end
  end

  private 
  def getUrl(url, timeout)
    url = URI.parse(url)
    req = Net::HTTP::Get.new(url.to_s)
    http = Net::HTTP.new(url.host, url.port)
    http.open_timeout = timeout.to_f/1000
    http.read_timeout = timeout.to_f/1000
    begin
      res=http.request(req)
      if Net::HTTPSuccess
        json=JSON.parse(res.body)
        return json
      end
    rescue
       return nil
    end
  end

  private
  def addToEvent(event, json, fields_to_merge)
    json.each do |key, value|
      if !fields_to_merge.any? || fields_to_merge.include?(key)
        if event[key].nil?
          event[key] = value
        elsif  (( ![ "@timestamp", "tags", "@version" ].include?(key) && event[key] != value ))
          event["#{key}-2"] = value
        elsif (( key == "tags" ))
          value.each do |tag|
            (event["tags"] ||= []) << tag
          end
        end
      end
    end
    return event
  end
end
