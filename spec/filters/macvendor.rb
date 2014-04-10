# encoding: utf-8

require "test_utils"
require "logstash/filters/macvendor"

describe LogStash::Filters::MacVendor do
  extend LogStash::RSpec

  describe "Simple mac to vendor test" do
    config <<-CONFIG
      filter {
        macvendor {
          source => "mac"
        }
      }
    CONFIG

  sample("mac" => "60:36:dd:db:63:25") do
      insist { subject["macvendor"] } == "Intel Corporate"
    end
  end

  describe "Alternative target" do
    config <<-CONFIG
      filter {
        macvendor {
          source => "mac"
          target => "alternative"
        }
      }
    CONFIG

  sample("mac" => "60:36:dd:db:63:25") do
      insist { subject["alternative"] } == "Intel Corporate"
    end
  end

  describe "Online database" do
    require 'open-uri'
    begin
      if open("http://standards.ieee.org")
        config <<-CONFIG
          filter {
            macvendor {
              source => "mac"
              local => false
            }
          }
        CONFIG

      sample("mac" => "60:36:dd:db:63:25") do
          insist { subject["macvendor"] } == "Intel Corporate"
        end
      end
    rescue
      pp "IEEE website unavailable, test skipped"
    end
  end
end