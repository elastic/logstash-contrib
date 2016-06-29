# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::NagiosPerfData < LogStash::Filters::Base

  # Creates individual events from Nagios host or service performance data.
  # String values that test true as float are output as float.
  #
  # For example, a service performance data string ...
  #
  # rta=0.152ms;3000.000;5000.000;0; pl=0%;80;100;; rtmax=0.214ms;;;; rtmin=0.103ms;;;;
  #
  # results in the following individual events:
  #
  # "rta value": 0.152
  # "rta uom": "ms"
  # "rta warn": 3000
  # "rta crit": 5000
  # "rta min": 0
  # "pl value": 0
  # "pl uom": "%"
  # "pl warn": 80
  # "pl crit": 100
  # "rtmax value": 0.214
  # "rtmax uom": "ms"
  # "rtmin value": 0.103
  # "rtmin uom": "ms"
  #
  # Example usage
  #
  # filter {
  #   nagios_perfdata {
  #     source => [ "nagios_serviceperfdata" ]
  #   }
  # }
  #
  # filter {
  #   nagios_perfdata {
  #     source => [ "nagios_hostperfdata" ]
  #   }
  # }

  config_name "nagios_perfdata"

  milestone 1

  config :source, :validate => :string

  public
  def register
    # nothing to do
  end # def register

  public
  def is_float(x)
    true if Float(x) rescue false
  end

  public
  def is_empty(x)
    true if x.empty? rescue false
  end

  public
  def filter(event)
    return unless filter?(event)

    metrics = Array.new

    # Scan iterates over the string with a regex and translates each
    # space to 3 colons in the first captured parens (label) then pushes
    # the translated value concatenated with the second captured parens
    # value to metrics array

    event[@source].scan(/([^=]*)=(\S*)(\s+)?/) { |x, y| metrics.push("#{x.gsub("\s", ':::')}=#{y}") }

    metrics = metrics.join(" ")

    metrics.split.each do |m|

      a = Array.new

      # capturing parens as follows
      # label = $1
      # value = $2
      # uom   = $3
      # warn  = $4
      # crit  = $5
      # min   = $6
      # max   = $7

      m.sub!(/([^=]*)=(-?\d*[.]?\d*)([^;]*)?;?([^;]*)?;?([^;]*)?;?([^;]*)?;?([^;]*)?/) {
        a.push("#{$1}", "#{$2}", "#{$3}", "#{$4}", "#{$5}", "#{$6}", "#{$7}")
      }

      # array a elements as follows
      # label = 0
      # value = 1
      # uom   = 2
      # warn  = 3
      # crit  = 4
      # min   = 5
      # max   = 6

      # label and value are not optional

      if a[0].nil? or is_empty(a[0])
        raise "failed to parse the label from #{@source}"
      end

      if a[1].nil? or is_empty(a[1])
        raise "failed to parse the value after parsing the label from #{@source}"
      end

      # String to float where applicable
      a.map!{ |x| x = x.to_f if is_float(x); x }

      # Convert label back to original value
      a[0].gsub!(':::', "\s")

      event["#{a[0]} value"] = a[1]
      filter_matched(event)

      # uom, warn, crit, min, max are optional in performance data output
      # https://nagios-plugins.org/doc/guidelines.html

      event["#{a[0]} uom"]   = a[2] if !a[2].nil? and !is_empty(a[2])
      filter_matched(event)

      event["#{a[0]} warn"]  = a[3] if !a[3].nil? and !is_empty(a[3])
      filter_matched(event)

      event["#{a[0]} crit"]  = a[4] if !a[4].nil? and !is_empty(a[4])
      filter_matched(event)

      event["#{a[0]} min"]   = a[5] if !a[5].nil? and !is_empty(a[5])
      filter_matched(event)

      event["#{a[0]} max"]   = a[6] if !a[6].nil? and !is_empty(a[6])
      filter_matched(event)

    end

  end # def filter

end # class LogStash::Filters::NagiosPerfData
