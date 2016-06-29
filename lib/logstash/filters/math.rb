require "logstash/filters/base"
require "logstash/namespace"
# Do various simple math operations
# Configuration:
# filter {
#   math {
#     calculate => [
#         [ "add", "a_field1", "a_field2", "a_target" ],   # a + b => target
#         [ "sub", "b_field1", "b_field2", "b_target" ],   # a - b => target
#         [ "div", "c_field1", "c_field2", "c_target" ],   # a / b => target
#         [ "mpx", "d_field1", "d_field2", "d_target" ]    # a * b => target
#     ]
#   }
# }
# Multiple calculations can be executed with one call
#
# Sequence of processing is as they are listed, so processing of just-generated fields is
# possible as long as it is done in the correct sequence.
#
# Works with float and integer values

class LogStash::Filters::Math < LogStash::Filters::Base
  config_name "math"
  milestone 1

  # fields - second subtracted from the first
  config :calculate, :validate => :array, :required => true

  public
  def register
    # Do some sanity checks that calculate is actually an array-of-arrays, and that each calculation (sub-array)
    # is exactly 4 fields and the first field is a valid calculation opperator name.
    for calculation in calculate do
      if calculation.length % 4 != 0
        abort("Each calculation must have 4 elements, this one had " + calculation.length + " " + calculation.to_s )
      end # end calculaction.length is 4
      if ! calculation[0].match('^(add|sub|div|mpx)$' )
        abort("First element of a calculation must be add|sub|div|mpx, but is: " + calculation[0] )
      end # if calculation[0] valid
    end # for each calculate
  end # def register

  public
  def filter(event)
    return unless filter?(event)
    for calculation in calculate do
      # Check that all the fields exist and are numeric
      next unless event.include?(calculation[1])
      next unless event.include?(calculation[2])
      next unless event[calculation[1]].is_a? Float or event[calculation[2]].is_a? Integer
      next unless event[calculation[1]].is_a? Float or event[calculation[2]].is_a? Integer
      case calculation[0]
      when "add"
        event[calculation[3]] = event[calculation[1]] + event[calculation[2]]
      when "sub"
        event[calculation[3]] = event[calculation[1]] - event[calculation[2]]
      when "div"
        # Avoid division by zero
        next if event[calculation[2]] == 0
        event[calculation[3]] = event[calculation[1]].to_f / event[calculation[2]]
      when "mpx"
        event[calculation[3]] = event[calculation[1]] * event[calculation[2]]
      end # case calculation[0]
    end # for each calculate
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::Math


