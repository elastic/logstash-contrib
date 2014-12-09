require "test_utils"
require "logstash/filters/yaml"

describe LogStash::Filters::Yaml do
  extend LogStash::RSpec

  describe "parse message into the event" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML, store the results in the 'data' field'
          source => "message"
        }
      }
    CONFIG

    sample '{ hello: "world", list: [ 1, 2, 3 ], hash: { "k": "v" } }' do
      insist { subject["hello"] } == "world"
      insist { subject["list" ] } == [1,2,3]
      insist { subject["hash"] } == { "k" => "v" }
    end
  end

  describe "parse message into a target field" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML, store the results in the 'data' field'
          source => "message"
          target => "data"
        }
      }
    CONFIG

    sample '{ hello: "world", list: [ 1, 2, 3 ], hash: { "k": "v" } }' do
      insist { subject["data"]["hello"] } == "world"
      insist { subject["data"]["list" ] } == [1,2,3]
      insist { subject["data"]["hash"] } == { "k" => "v" }
    end
  end

  describe "tag invalid yaml" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML, store the results in the 'data' field'
          source => "message"
          target => "data"
        }
      }
    CONFIG

    sample "invalid yaml" do
      insist { subject["tags"] }.include?("_yamlparsefailure")
    end
  end

  describe "fixing @timestamp" do
    config <<-CONFIG
      filter {
        yaml {
          source => "message"
        }
      }
    CONFIG

    sample "{ \"@timestamp\": \"2013-10-19T00:14:32.996Z\" }" do
      insist { subject["@timestamp"] }.is_a?(Time)
      insist { subject["@timestamp"].to_json } == "\"2013-10-19T00:14:32.996Z\""
    end
  end
end
