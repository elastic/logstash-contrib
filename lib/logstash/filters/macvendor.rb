# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# The MacVendor filter uses the OUI database (Organizationally Unique Identifier)
# to match a MAC address to its vendor.
# It relies on the "mac_vendor" ruby gem which can either use an
# included database, or fetch the latest one from the IEEE website when initialized.
# The config should look like this:
#
# filter {
#   macvendor {
#     source => "mac_source_field"
#     local => true
#     target => "target_field"
#   }
# }
#
class LogStash::Filters::MacVendor < LogStash::Filters::Base

  config_name "macvendor"

  milestone 1

  # The field containing the MAC address. If this field is
  # an array, only the first value will be used.
  config :source, :validate => :string, :required => true

  # Use the included local database instead of fetching the latest one on internet
  config :local, :validate => :boolean, :default => true

  # Specify field into which Logstash should store the mac vendor data.
  config :target, :validate => :string, :default => 'macvendor'

  public
  def register
    require "mac_vendor"
    @v = MacVendor.new :use_local => @local
    @v.preload_cache unless @local
  end # def register

  public
  def filter(event)
    # return nothing unless there's an actual filter event
    return unless filter?(event)
    mac = event[@source]
    mac = mac.first if mac.is_a? Array
    if mac.is_a?(String)
      vendor = @v.lookup mac
      unless vendor.nil?
        vendor = vendor[:name] 
        unless @local
          # Fix encoding with online database
          vendor = case vendor.encoding
            when Encoding::ASCII_8BIT; vendor.force_encoding(Encoding::ISO_8859_1).encode(Encoding::UTF_8)
            when Encoding::ISO_8859_1, Encoding::US_ASCII;  vendor.encode(Encoding::UTF_8)
            else; vendor
          end
        end
        event[@target] = vendor
      end
    end
    # filter_matched should go in the last line of our successful code 
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::MacVendor