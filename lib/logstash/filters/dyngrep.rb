# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


# Non-regex grep in a specific field. A text is matched if any of the pattern
# specified in pattern_file is matching (substring of the source text).
#
# Events not matched can be dropped. If 'negate' is set to true (defaults false),
# then matching events are dropped.
class LogStash::Filters::DynGrep < LogStash::Filters::Base

  config_name "dyngrep"
  milestone 0

  # Drop events that don't match
  config :drop, :validate => :boolean, :default => false

  # Negate the match. Similar to 'grep -v'
  #
  # If this is set to true, then any positive matches will result in the
  # event being cancelled and dropped. Non-matching will be allowed
  # through.
  config :negate, :validate => :boolean, :default => false

  # Field where the grep happens
  config :field, :validate => :string, :required => true

  # Patterns for grepping
  #
  # For example:
  #      filter {
  #        dyngrep {
  #          field => "message"
  #          pattern_file => "/etc/patterns.conf"
  #        }
  #      }
  config :pattern_file, :validate => :string, :required => true

  # Timer how often should the configuration to be re-read
  #
  config :refresh_interval, :validate => :number, :default => 300

  public
  def register
    @patterns = load_patterns(true)
    @next_refresh = Time.now + @refresh_interval
  end # def register

  public
  def load_patterns(registering=false)
    begin
      return File.readlines(@pattern_file).map {|item| item.strip}.find_all { |item| !item.empty? }
    rescue Exception => e
      if registering
        raise e
      else
        @logger.warn("Failed to read pattern files: #{e}")
        return @patterns
      end
    end
  end

  public
  def filter(event)
    return unless filter?(event)

    if @next_refresh < Time.now
      @logger.info("Refreshing patterns")
      @patterns = load_patterns(false)
      @next_refresh = Time.now + @refresh_interval
    end

    if @negate && @patterns.empty? && @drop
      event.cancel
      return
    end

    if event[@field].nil?
      if @negate
        # match
        filter_matched(event)
      elsif @drop
        event.cancel
      end
    else
      matches = 0
      source = event[@field].is_a?(Array) ? event[@field].first.to_s : event[@field].to_s
      @patterns.each do |pattern|
        if source.include? pattern
          matches = 1
          break
        end
      end
      if matches > 0
        if @negate
          event.cancel if @drop
        else
          filter_matched(event)
        end
      else
        if @negate
          filter_matched(event)
        else
          event.cancel if drop
        end
      end
    end
  end
end # class LogStash::Filters::Grep
