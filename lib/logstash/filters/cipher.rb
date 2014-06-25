# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "openssl"


# This filter parses a source and apply a cipher or decipher before
# storing it in the target.
#
class LogStash::Filters::Cipher < LogStash::Filters::Base
  config_name "cipher"
  milestone 1

  # The field to perform filter
  #
  # Example, to use the @message field (default) :
  #
  #     filter { cipher { source => "message" } }
  config :source, :validate => :string, :default => "message"

  # The name of the container to put the result
  #
  # Example, to place the result into crypt :
  #
  #     filter { cipher { target => "crypt" } }
  config :target, :validate => :string, :default => "message"

  # Do we have to perform a base64 decode or encode?
  #
  # If we are decrypting, base64 decode will be done before.
  # If we are encrypting, base64 will be done after.
  #
  config :base64, :validate => :boolean, :default => true

  # The key to use
  config :key, :validate => :string

  # The key size to pad
  #
  # It depends of the cipher algorythm.I your key don't need
  # padding, don't set this parameter
  #
  # Example, for AES-256, we must have 32 char long key
  #     filter { cipher { key_size => 32 }
  #
  config :key_size, :validate => :number, :default => 32

  # The character used to pad the key
  config :key_pad, :default => "\0"

  # The cipher algorythm
  #
  # A list of supported algorithms can be obtained by
  #
  #     puts OpenSSL::Cipher.ciphers
  config :algorithm, :validate => :string, :required => true

  # Encrypting or decrypting some data
  #
  # Valid values are encrypt or decrypt
  config :mode, :validate => :string, :required => true

  # Cypher padding to use. Enables or disables padding. 
  #
  # By default encryption operations are padded using standard block padding 
  # and the padding is checked and removed when decrypting. If the pad 
  # parameter is zero then no padding is performed, the total amount of data 
  # encrypted or decrypted must then be a multiple of the block size or an 
  # error will occur.
  #
  # See EVP_CIPHER_CTX_set_padding for further information.
  #
  # We are using Openssl jRuby which uses default padding to PKCS5Padding
  # If you want to change it, set this parameter. If you want to disable
  # it, Set this parameter to 0
  #     filter { cipher { cipher_padding => 0 }}
  config :cipher_padding, :validate => :string

  # The initialization vector to use (statically hard-coded). For 
  # a random IV see the iv_random_length property
  #
  # NOTE: If iv_random_length is set, it takes precedence over any value set for "iv"
  #
  # The cipher modes CBC, CFB, OFB and CTR all need an "initialization
  # vector", or short, IV. ECB mode is the only mode that does not require
  # an IV, but there is almost no legitimate use case for this mode
  # because of the fact that it does not sufficiently hide plaintext patterns.
  #
  # For AES algorithms set this to a 16 byte string. 
  #  
  # 	filter { cipher { iv => "1234567890123456" }} 
  config :iv, :validate => :string
  
  # Force an random IV to be used per encryption invocation and specify
  # the length of the random IV that will be generated via:
  #
  #			 OpenSSL::Random.random_bytes(int_length)
  #
  # If iv_random_length is set, it takes precedence over any value set for "iv"
  #
  # Enabling this will force the plugin to generate a unique
  # random IV for each encryption call. This random IV will be prepended to the 
  # encrypted result bytes and then base64 encoded. On decryption "iv_random_length" must 
  # also be set to utilize this feature. Random IV's are better than statically
  # hardcoded IVs
  #
  # For AES algorithms you can set this to a 16
  #  
  # 	filter { cipher { iv_random_length => 16 }} 
  config :iv_random_length, :validate => :number
  
  def register
    require 'base64' if @base64
    init_cipher
  end # def register


  def filter(event)
    return unless filter?(event)


    #If decrypt or encrypt fails, we keep it it intact.
    begin
      
      if (event[@source].nil? || event[@source].empty?) 
        @logger.debug("Event to filter, event 'source' field: " + @source + " was null(nil) or blank, doing nothing")
      	return
      end
    
      #@logger.debug("Event to filter", :event => event)
      data = event[@source]
      if @mode == "decrypt"
        data =  Base64.decode64(data) if @base64 == true
        
        if !@iv_random_length.nil? 
        	@random_iv = data.byteslice(0,@iv_random_length)
        	data = data.byteslice(@iv_random_length..data.length)
        end
        
      end
      
      if !@iv_random_length.nil? and @mode == "encrypt"
      	 @random_iv = OpenSSL::Random.random_bytes(@iv_random_length)
      end
      
      # if iv_random_length is specified, generate a new one
      # and force the cipher's IV = to the random value
      if !@iv_random_length.nil? 
         @cipher.iv = @random_iv
      end
      
      result = @cipher.update(data) + @cipher.final
      
      if @mode == "encrypt"
      
        # if we have a random_iv, prepend that to the crypted result
      	if !@random_iv.nil? 
        	result = @random_iv + result
        end
        
        result =  Base64.encode64(result) if @base64 == true
      end
      
    rescue => e
      @logger.warn("Exception catch on cipher filter", :event => event, :error => e)
    else
      event[@target]= result
      #Is it necessary to add 'if !result.nil?' ? exception have been already catched.
      #In doubt, I keep it.
      filter_matched(event) if !result.nil?
      #Too much bad result can be a problem, reinit cipher prevent this.
      init_cipher
    end
  end # def filter

  def init_cipher

    @cipher = OpenSSL::Cipher.new(@algorithm)
    if @mode == "encrypt"
      @cipher.encrypt
    elsif @mode == "decrypt"
      @cipher.decrypt
    else
      @logger.error("Invalid cipher mode. Valid values are \"encrypt\" or \"decrypt\"", :mode => @mode)
      raise "Bad configuration, aborting."
    end

    if @key.length != @key_size
      @logger.debug("key length is " + @key.length.to_s + ", padding it to " + @key_size.to_s + " with '" + @key_pad.to_s + "'")
      @key = @key[0,@key_size].ljust(@key_size,@key_pad)
    end

    @cipher.key = @key

	if !@iv.nil? and !@iv.empty? and @iv_random_length.nil?
	    @cipher.iv = @iv if @iv
	    
	elsif !@iv_random_length.nil?
		@logger.debug("iv_random_length is configured, ignoring any statically defined value for 'iv'", :iv_random_length => @iv_random_length)
		
	else 
		raise "cipher plugin: either 'iv' or 'iv_random_length' must be configured, but not both; aborting"
	end

    @cipher.padding = @cipher_padding if @cipher_padding

    @logger.debug("Cipher initialisation done", :mode => @mode, :key => @key, :iv => @iv, :iv_random => @iv_random, :cipher_padding => @cipher_padding)
  end # def init_cipher


end # class LogStash::Filters::Cipher
