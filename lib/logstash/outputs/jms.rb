# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# Write events to a Jms Broker. Supports both Jms Queues and Topics.
#
# For more information about Jms, see <http://docs.oracle.com/javaee/6/tutorial/doc/bncdq.html>
# For more information about the Ruby Gem used, see <http://github.com/reidmorrison/jruby-jms>
# Here is a config example :
#	 jms {
#			include_header => false
#			include_properties => false
#			include_body => true
#			use_jms_timestamp => false
#			queue_name => "myqueue"
#			yaml_file => "~/jms.yml"
#			yaml_section => "mybroker"
#		}
#
#
class LogStash::Outputs::Jms < LogStash::Outputs::Base
	config_name "jms"
	milestone 1

# Initial connection timeout in seconds.
config :timeout, :validate => :number, :default => 1000

# Name of delivery mode to use
# Options are "persistent" and "non_persistent" if not defined nothing will be passed.
# TODO(AlphaCluster) need to reconfigure delivery mode
#config :delivery_mode, :validate => :string, :default => nil

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

# If you do not use an yaml configuration use either the factory or jndi_name.

# An optional array of Jar file names to load for the specified
# JMS provider. By using this option it is not necessary
# to put all the JMS Provider specific jar files into the
# java CLASSPATH prior to starting Logstash.
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
		
		elsif @jndi_name
			@jms_config = {
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
		
		@logger.debug("JMS Config being used", :context => @jms_config)
		@connection = JMS::Connection.new(@jms_config)
		@session = @connection.create_session()

		# Cache the producer since we should keep reusing this one.
		if @queue_name.nil?
			@producer = @session.create_producer(@session.create_destination(:topic_name => @topic_name))
		else
			@producer = @session.create_producer(@session.create_destination(:queue_name => @queue_name))
		end

		if !@delivery_mode.nil?
			@producer.delivery_mode_sym = @deliver_mode
		end
	end # def register

	def receive(event)
			return unless output?(event)

			begin
				@producer.send(@session.message(event.to_json))
#				if @queue_name
#					@jms_session_pool.producer(:queue_name => @queue_name) do |session, producer|
#						producer.send(session.message("Hello World"))
#					end
#				else
#					@jms_session_pool.producer(:topic_name => @topic_name) do |session, producer|
#						producer.send(session.message("Hello World"))
#					end
#				end

			rescue LogStash::ShutdownSignal => e
				@producer.close()
				@session.close()
				@connection.close()
			rescue => e
				@logger.warn("Failed to send event to JMS", :event => event, :exception => e,
										 :backtrace => e.backtrace)
			end
	end # def receive
end # class LogStash::Output::Jms
