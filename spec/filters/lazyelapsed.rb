# encoding: utf-8
require 'socket'
require "logstash/filters/lazyelapsed"

describe LogStash::Filters::LazyElapsed do
  START_TAG = "event_start"
  END_TAG   = "event_end"
  UNIQUE_ID_FIELD  = "unique_id_field"

  it "raises an error if required config isn't there" do
    # missing timestamp_field.
    config = {"start_tag" => START_TAG, "end_tag" => END_TAG, "unique_id_field" => UNIQUE_ID_FIELD}
    expect { LogStash::Filters::LazyElapsed.new(config) }.to raise_error

    # missing unique_id_field.
    config = {"start_tag" => START_TAG, "end_tag" => END_TAG, "timestamp_field" => "@timestamp"}
    expect { LogStash::Filters::LazyElapsed.new(config) }.to raise_error

    # missing end_tag.
    config = {"start_tag" => START_TAG, "unique_id_field" => UNIQUE_ID_FIELD, "timestamp_field" => "@timestamp"}
    expect { LogStash::Filters::LazyElapsed.new(config) }.to raise_error

    # missing start.
    config = {"end_tag" => END_TAG, "unique_id_field" => UNIQUE_ID_FIELD, "timestamp_field" => "@timestamp"}
    expect { LogStash::Filters::LazyElapsed.new(config) }.to raise_error
  end


  it "saves off the start event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp"} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message"
    }
    
    start_event = LogStash::Event.new(event_data)

    filter.filter(start_event)

    insist { filter.start_events.size } == 1
    insist { filter.start_events["1"] } != nil 
    insist { filter.start_events["1"].start } != nil
    insist { filter.start_events["1"].start["tags"][0] } == START_TAG
    insist { filter.start_events["1"].start["message"] } == event_data["message"]
  end

  
  it "generates a new error event and copies the requested fields from the start event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_start" => ["message", "message2"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(event_data)

    filter.filter(start_event)
    filter.flush()
  
    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_error"
    insist { event["message"] } == event_data["message"]
    insist { event["message2"] } == event_data["message2"]
  end


  it "does nothing if the field is not there to copy from the start event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_start" => ["message3", "message4"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(event_data)

    filter.filter(start_event)
    filter.flush()
  
    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_error"
    insist { event["message3"] } == nil
    insist { event["message4"] } == nil
  end


  it "saves the correct end event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_start" => ["message3", "message4"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data1 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "2",
      "message2" => "another extra log message"
    }

    end_event1 = LogStash::Event.new(end_event_data1)

    end_event_data2 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message3" => "some extra log message"
    }

    end_event2 = LogStash::Event.new(end_event_data2)

    filter.filter(start_event)
    filter.filter(end_event1)
    filter.filter(end_event2)

    insist { filter.start_events["1"].end } != nil
    insist { filter.start_events["1"].end["tags"][0] } == END_TAG
    insist { filter.start_events["1"].end["message3"] } == end_event_data2["message3"]
    insist { filter.start_events["1"].end["message2"] } == nil
  end
  

  it "is lazy about the end event to save" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_start" => ["message3", "message4"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data1 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message3" => "some extra log message"
    }

    end_event1 = LogStash::Event.new(end_event_data1)

    end_event_data2 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message4" => "This should be the end event we pick"
    }

    end_event2 = LogStash::Event.new(end_event_data2)

    filter.filter(start_event)
    filter.filter(end_event1)
    filter.filter(end_event2)

    insist { filter.start_events["1"].end } != nil
    insist { filter.start_events["1"].end["tags"][0] } == END_TAG
    insist { filter.start_events["1"].end["message3"] } == nil
    insist { filter.start_events["1"].end["message4"] } == end_event_data2["message4"]
  end


  it "generates a new match event and copies the requested fields from the end event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_end" => ["message3", "message4"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message3" => "some extra log message",
      "message4" => "another log message"
    }

    end_event = LogStash::Event.new(end_event_data)

    filter.filter(start_event)
    filter.filter(end_event)

    filter.start_events["1"].end["@timestamp"] = filter.start_events["1"].start["@timestamp"] + 34

    filter.flush()
  
    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["message3"] } == end_event_data["message3"]
    insist { event["message4"] } == end_event_data["message4"]
    insist { event["message"] } == nil
    insist { event["message2"] } == nil
    insist { event["elapsed_time"] } == 34000
  end


  it "does nothing if the field is not there to copy from the end event" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_copy_from_end" => ["message", "message3", "message4"],
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message",
      "message2" => "some other log message"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some extra log message",
      "message2" => "another log message"
    }

    end_event = LogStash::Event.new(end_event_data)

    filter.filter(start_event)
    filter.filter(end_event)

    filter.flush()
  
    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["message"] } == end_event_data["message"] 
    insist { event["message"] } != start_event_data["message"] 
    insist { event["message3"] } == nil 
    insist { event["message4"] } == nil
  end


  it "increments age correctly on a flush" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp"} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "some log message"
    }
    
    start_event = LogStash::Event.new(event_data)

    filter.filter(start_event)

    insist { filter.start_events["1"].age } == 0
    
    filter.flush()
    insist { filter.start_events["1"].age } == 5

    filter.flush()
    insist { filter.start_events["1"].age } == 10
  end


  it "records fields from the events correctly" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_record" => {"messages" => "message", 
                                      "messages2" => "message2",
                                      "messages3" => "message3"},
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "first message",
      "message2" => "first message2"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data1 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "second message",
      "message4" => "we shouldn't see this one"
    }

    end_event1 = LogStash::Event.new(end_event_data1)

    end_event_data2 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "message" => "third message",
      "message2" => "second message2"
    }

    end_event2 = LogStash::Event.new(end_event_data2)

    filter.filter(start_event)
    filter.filter(end_event1)
    filter.filter(end_event2)

    filter.flush()

    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["messages"].size } == 3
    insist { event["messages"][0] } == start_event_data["message"] 
    insist { event["messages"][1] } == end_event_data1["message"] 
    insist { event["messages"][2] } == end_event_data2["message"] 
    insist { event["messages2"].size } == 2 
    insist { event["messages2"][0] } == start_event_data["message2"] 
    insist { event["messages2"][1] } == end_event_data2["message2"] 
    insist { event["messages3"] } == nil
    insist { event["message4"] } == nil
  end 


  it "sums fields from the events correctly" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "@timestamp",
               "fields_to_sum" => {"sum" => "time", 
                                   "sum2" => "time2",
                                   "sum3" => "time3"},
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "time" => 23,
      "time2" => 85
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data1 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "time" => 18,
      "time4" => 1345
    }

    end_event1 = LogStash::Event.new(end_event_data1)

    end_event_data2 = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "time" => 10045,
      "time2" => 90094
    }

    end_event2 = LogStash::Event.new(end_event_data2)

    filter.filter(start_event)
    filter.filter(end_event1)
    filter.filter(end_event2)

    filter.flush()

    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["sum"] } == start_event_data["time"] + end_event_data1["time"] + end_event_data2["time"]
    insist { event["sum2"] } == start_event_data["time2"] + end_event_data2["time2"]
    insist { event["sum3"] } == nil
    insist { event["time4"] } ==  nil
  end

  it "supports the timestamp being a string" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "mytimestamp",
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "mytimestamp" => "Wed Aug 27 16:03:41 MDT 2014"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "mytimestamp" => "2014/08/27 16:05:43"
    }

    end_event = LogStash::Event.new(end_event_data)

    filter.filter(start_event)
    filter.filter(end_event)

    filter.flush()

    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["elapsed_time"] } == 122000 
  end

  it "doesn't crash if it can't parse the timestamp string" do
    config = {"start_tag" => START_TAG,
               "end_tag" => END_TAG,
               "unique_id_field" => UNIQUE_ID_FIELD,
               "timestamp_field" => "mytimestamp",
               "time_to_wait" => 2} 
    filter = LogStash::Filters::LazyElapsed.new(config)
    filter.register
  
    start_event_data = {
      "tags" => [ START_TAG ],
      UNIQUE_ID_FIELD => "1",
      "mytimestamp" => "This is not a timestamp"
    }
    
    start_event = LogStash::Event.new(start_event_data)

    end_event_data = {
      "tags" => [ END_TAG ],
      UNIQUE_ID_FIELD => "1",
      "mytimestamp" => "dsfhjkekldddkskskjdj"
    }

    end_event = LogStash::Event.new(end_event_data)

    filter.filter(start_event)
    filter.filter(end_event)

    filter.flush()
    event = filter.last_new_event
    insist { event } != nil
    insist { event["tags"][0] } == "lazy_elapsed_match"
    insist { event["elapsed_time"] } == 0 
  end
end
