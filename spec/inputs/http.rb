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

describe "inputs/http" do
  extend LogStash::RSpec

  describe "Single client connection" do
    event_count = 10
    port = 53215
    config <<-CONFIG
    input {
      http {
        type => "blah"
        port => #{port}
      }
    }
    CONFIG

    url = URI.parse("http://localhost:#{port}/test")

    headers = {"X-Forwarded-For" => "192.168.2.1"}

    http = Net::HTTP.new(url.host, url.port)
    input do |pipeline, queue|
      th = Thread.new { pipeline.run }
      sleep 0.1 while !pipeline.ready?
      sleep 0.1 until port_ready?(port)

      event_count.times do |value|
        http.post(url.path, {"message" => "Hello #{value}"}.to_json, headers)
      end

      events = event_count.times.collect { queue.pop }
      event_count.times do |i|
        insist { events[i]["message"] } == "Hello #{i}"
        insist { events[i]["source_host"] } == "127.0.0.1"
        insist { events[i]["http"]["x-forwarded-for"] } == "192.168.2.1"
        insist { events[i]["http"]["path"] } == "/test"
      end

      pipeline.shutdown
      th.join
    end # input
  end
end
