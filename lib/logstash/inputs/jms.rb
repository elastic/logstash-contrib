# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
#require "jruby-jms"

# Read events from a Jms Broker. Supports both Jms Queues and Topics.
#
# For more information about Jms, see <http://docs.oracle.com/javaee/6/tutorial/doc/bncdq.html>
# For more information about the Ruby Gem used, see <http://github.com/reidmorrison/jruby-jms>
# Here is a config example :
#	 jms {
#			include_header => false
#			include_properties => false
#			include_body => true
#			use_jms_timestamp => false
#			interval => 10
#			queue_name => "myqueue"
#			yaml_file => "~/jms.yml"
#			yaml_section => "mybroker"
#		}
#
#
class LogStash::Inputs::Jms < LogStash::Inputs::Base
	config_name "jms"
	milestone 1

	default :config, "plain"

	# A JMS message has three parts :
	#	 Message Headers (required)
	#	 Message Properties (optional)
	#	 Message Bodies (optional)
	# You can tell the input plugin which parts should be included in the event produced by Logstash
	#
	# Include JMS Message Header Field values in the event
	config :include_header, :validate => :boolean, :default => true
	# Include JMS Message Properties Field values in the event
	config :include_properties, :validate => :boolean, :default => true
	# Include JMS Message Body in the event
	# Supports TextMessage and MapMessage
	# If the JMS Message is a TextMessage, then the value will be in the "message" field of the event
	# If the JMS Message is a MapMessage, then all the key/value pairs will be added in the Hashmap of the event
	# BytesMessage, StreamMessage and ObjectMessage are not supported
	config :include_body, :validate => :boolean, :default => true
	# Convert the JMSTimestamp header field to the @timestamp value of the event
	# Don't use it for now, it is buggy
	config :use_jms_timestamp, :validate => :boolean, :default => false

	# Choose an implementation of the run block. Value can be either consumer, async or thread
	config :runner, :validate => :string, :required => true, :default => "consumer"

	# Set the selector to use to get messages off the queue or topic
	config :selector, :validate => :string

	# Initial connection timeout in seconds.
	config :timeout, :validate => :number, :default => 1000

	# Polling interval.
	# This is the time sleeping between asks to a consumed Queue.
	# This parameter has non influence in the case of a subcribed Topic.
	config :interval, :validate => :number, :default => 10

	# Name of the Queue to consume.
	# Mandatory unless :topic_name is supplied
	config :queue_name, :validate => :string
	# Name of the Topic to subscribe to.
	# Mandatory unless :queue_name is supplied
	config :topic_name, :validate => :string

	# Yaml config file
	config :yaml_file, :validate => :string
	# Yaml config file section name
	# For some known examples, see: [Example jms.yml](https://github.com/reidmorrison/jruby-jms/blob/master/examples/jms.yml)
	config :yaml_section, :validate => :string



	# TODO(claveau): here is the the problem to deal with ...
	# Each JMS broker comes with its own API, libraries and config parameters.
	# The jruby-jms expects an array of properties where some are normalized, and some are specific.

	# So, for this plugin, can we just expect a Yaml config file,
	# or should we expose all the properties like the following ...

	# An optional array of Jar file names to load for the specified
	# JMS provider. By using this option it is not necessary
	# to put all the JMS Provider specific jar files into the
	# environment variable CLASSPATH prior to starting Logstash
	config :require_jars, :validate => :array

	# Name of JMS Provider Factory class
	config :factory, :validate => :string
	# Username to connect to JMS provider with
	config :username, :validate => :string
	# Password to use when connecting to the JMS provider
	config :password, :validate => :string
	# Url to use when connecting to the JMS provider
	config :broker_url, :validate => :string

	# Name of JNDI entry at which the Factory can be found
	config :jndi_name, :validate => :string
	# Mandatory if jndi lookup is being used,
	# contains details on how to connect to JNDI server
	config :jndi_context, :validate => :hash

	# :yaml_file, :factory and :jndi_name are mutually exclusive, both cannot be supplied at the
	# same time. The priority order is :yaml_file, then :jndi_name, then :factory
	#
	# JMS Provider specific properties can be set if the JMS Factory itself
	# has setters for those properties.
	#
	# For some known examples, see: [Example jms.yml](https://github.com/reidmorrison/jruby-jms/blob/master/examples/jms.yml)

	public
	def register
		require "jms"
		@connection = nil

		if @yaml_file
			@jms_config = YAML.load_file(@yaml_file)[@yaml_section]

		# TODO(claveau): causes an exception
		# #<TypeError: can't dup NilClass> in /jms/connection.rb:172 (params.dup)
		elsif @jndi_name = {
				:require_jars => @require_jars,
				:jndi_name => @jndi_name,
				:jndi_context => @jndi_context}

		elsif @factory
			@jms_config = {
				:require_jars => @require_jars,
				:factory => @factory,
				:username => @username,
				:password => @password,
				:broker_url => @broker_url,
				:url => @broker_url # "broker_url" is named "url" with Oracle AQ
				}
		end

		@logger.debug @jms_config

	end # def register





	private
	def queue_event(msg, output_queue)
		begin
				event = LogStash::Event.new

				# Here, we can use the JMS Enqueue timestamp as the @timestamp
				# TODO(claveau): investigate the reason why the Java integer Timestamp conversion is erroneous
				# For example :
				#	 when the Enqueue real time is "2014-02-12T11:20:58+01:00"
				#	 we receive a msg.jms_timestamp => 1392200458000
				# 	then the conversion ::Time.at(msg.jms_timestamp) => "46087-01-28T06:26:40.000+01:00"
				if @use_jms_timestamp and msg.jms_timestamp
					event.timestamp = ::Time.at(msg.jms_timestamp)
				end

				if @include_header
				 #event.append(msg.attributes)
					msg.attributes.each do |field, value|
						event[field.to_s] = value
					end
				end

				if @include_properties
				 #event.append(msg.properties)
					msg.properties.each do |field, value|
						event[field.to_s] = value
					end
				end

				if @include_body
					if msg.java_kind_of?(JMS::MapMessage)
					 #event.append(msg.data)
						msg.data.each do |field, value|
							event[field.to_s] = value # TODO(claveau): needs codec.decode or converter.convert ?
						end

					elsif msg.java_kind_of?(JMS::TextMessage)
						@codec.decode(msg.to_s) do |event_message|
							# Copy out the header data into the message.
							event.to_hash.each do |k,v|
								event_message[k] = v
							end
							# Now lets overwrite the event.
							event = event_message
						end
					else
						@logger.error( "Unknown data type #{msg.data.class.to_s} in Message" )
					end
				end

				decorate(event)
				output_queue << event

		rescue => e # parse or event creation error
			@logger.error("Failed to create event", :message => msg, :exception => e,
										:backtrace => e.backtrace);
		end
	end



	# Consume all available messages on the queue
	# sleeps some time, then consume again
	private
	def run_consumer(output_queue)
		JMS::Connection.session(@jms_config) do |session|
			while(true)
				session.consume(:queue_name => @queue_name, :timeout=>@timeout, :selector => @selector) do |message|
					queue_event message, output_queue
				end
			sleep @interval
			end
		end
	rescue LogStash::ShutdownSignal
		# Do nothing, let us quit.
	rescue => e
		@logger.warn("JMS Consumer died", :exception => e, :backtrace => e.backtrace)
		sleep(10)
		retry
	end # def run




	# Consume all available messages on the queue through a listener
	private
	def run_thread(output_queue)
		connection = JMS::Connection.new(@jms_config)
		connection.on_exception do |jms_exception|
			@logger.warn("JMS Exception has occurred: #{jms_exception}")
		end
		connection.on_message(:queue_name => @queue_name, :selector => @selector) do |message|
			queue_event message, output_queue
		end
		connection.start
		while(true)
			@logger.debug("JMS Thread sleeping ...")
			sleep @interval
		end
	rescue LogStash::ShutdownSignal
		connection.close
	rescue => e
		@logger.warn("JMS Consumer died", :exception => e, :backtrace => e.backtrace)
		sleep(10)
		retry
	end # def run



	# Consume all available messages on the queue through a listener
	private
	def run_async(output_queue)
		JMS::Connection.start(@jms_config) do |connection|
			# Define exception listener
			# The problem here is that we do not handle any exception
			connection.on_exception do |jms_exception|
				@logger.warn("JMS Exception has occurred: #{jms_exception}")
				raise jms_exception
			end
			# Define Asynchronous code block to be called every time a message is received
			connection.on_message(:queue_name => @queue_name, :selector => @selector) do |message|
				queue_event message, output_queue
			end
			# Since the on_message handler above is in a separate thread the thread needs
			# to do some other work. It will just sleep for 10 seconds.
			while(true)
				sleep @interval
			end
		end
	rescue LogStash::ShutdownSignal
		# Do nothing, let us quit.
	rescue => e
		@logger.warn("JMS Consumer died", :exception => e, :backtrace => e.backtrace)
		sleep(10)
		retry
	end # def run


	public
	def run(output_queue)
		case runner
			when "consumer" then
					run_consumer(output_queue)
			when "async" then
					run_async(output_queue)
			when "thread" then
					run_thread(output_queue)
		end
	end # def run

end # class LogStash::Inputs::Jms
