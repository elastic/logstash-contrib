require "logstash/filters/base"
require "logstash/namespace"
# Calculate the difference between two numeric fields
# Configuration:
# filter {
#   diff {
#     fields => [ "first", "second" ]
#     target => "diff"
#   }
# }
# the field "second" is subtracted from the field "first"
# e.g. for the event:
# { "type":"event", "first":12, "second":10}
# becomes
# { "type":"event", "first":12, "second":10, "diff": 2 }
#
# Works with float and integer values

class LogStash::Filters::Diff < LogStash::Filters::Base
  config_name "diff"
  milestone 1

  # fields - second subtracted from the first
  config :fields, :validate => :array, :required => true
  config :target, :validate => :string, :required => true

  public
  def register
    if fields.length != 2
      @logger.error( "Configuration 'fields must contain exactly two field names" )
    end
  end # def register

  public
  def filter(event)
    return unless filter?(event)
    diff(event)
    filter_matched(event)
  end # def filter

  def diff(event)
    next unless event.include?(fields[0])
    next unless event.include?(fields[1])
    if ! ( event[fields[0]].is_a? Float or event[fields[0]].is_a? Integer )
      @logger.error( "Not a number: ", :value => event[fields[0]] )
    end

    if ! ( event[fields[1]].is_a? Float or event[fields[1]].is_a? Integer )
      @logger.error( "Not a number: ", :value => event[fields[1]] )
    end
    event[target] = event[fields[0]] - event[fields[1]]
  end # def diff
end # class LogStash::Filters::Diff


