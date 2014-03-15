# coding: utf-8
require "test_utils"
require "net/http"
require "uri"

def port_ready?(port)
  TCPSocket.new('localhost', port)
  true
rescue Exception => e
  warn e
  false
end

describe "inputs/sns" do
  extend LogStash::RSpec

  describe "Single client connection" do
    event_count = 10
    port = 53215
    config <<-CONFIG
    input {
      sns {
        type => "blah"
        port => #{port}
      }
    }
    CONFIG

    url = URI.parse("http://localhost:#{port}/")

    headers = {"x-amz-sns-message-type" => "Notification", "x-amz-sns-topic-arn" => "arn:aws:sns:us-east-1:123456789012:MyTopic"}

    http = Net::HTTP.new(url.host, url.port)
    input do |pipeline, queue|
      th = Thread.new { pipeline.run }
      sleep 0.1 while !pipeline.ready?
      sleep 0.1 until port_ready?(port)

      event_count.times do |value|
        http.post(url.path, {"Message" => {"message" => "Hello #{value}"}.to_json, "Type" => "Notification"}.to_json, headers)
      end

      events = event_count.times.collect { queue.pop }
      event_count.times do |i|
        insist { events[i]["message"] } == "Hello #{i}"
      end

      pipeline.shutdown
      th.join
    end # input
  end
end
