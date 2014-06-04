# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/socket_peer"

# This input will fetch data from Zendesk and generate Logstash events for indexing into Elasticsearch.
# It will run continuously (sleeping 5 mins between each run) unless you are fetching all tickets from Zendesk.
# It uses version 1.3.5 of the official Zendesk ruby client api (zendesk_api) which is only compatible with 
# Zendesk api v2.  At the time of development, this input
# works with Logstash 1.4.x.  Currently, the input supports Zendesk organization, user, ticket and ticket comment objects.
# Requires a Zendesk admin token to run input.
#
# This input depends on the following gems:
# - zendesk_api
# - faraday (required by zendesk_api, see zendesk_api gemspec for details)
# - inflection (required by zendesk_api, see zendesk_api gemspec for details)
# - multi_json (required by zendesk_api, see zendesk_api gemspec for details)
# - hashie (required by zendesk_api, see zendesk_api gemspec for details)
# - mime-types (required by zendesk_api, see zendesk_api gemspec for details)
# - multipart-post (required by zendesk_api, see zendesk_api gemspec for details)
# - activesupport     
#
# Sample configuration:
# 
#input 
#{ 
#	zendesk
#	{
#		domain => "company.zendesk.com"
#		username => "user@company.com"
#		api_token => "your_zendesk_api_token"
#		api_logger => false
#		fetch_organizations => true
#		fetch_users => true
#		fetch_tickets => true
#		fetch_tickets_last_updated_n_days_ago => 1
#		fetch_comments => false
#		append_comments_to_tickets => false
#	}
#}
#
#filter {
#
#  # Constructs the geoJSON array Kibana 3 expects for bettermap
#  if [metadata][system][longitude]
#  {
#  mutate {
#    	add_field => {"geocode" => ["%{[metadata][system][longitude]}","%{[metadata][system][latitude]}"]}
#     }
#  # Convert back to float array
#  mutate {
#    	convert => ["geocode", "float"]
#     }     
#  }
#
#  # Zendesk incremental export api returns all integer fields as strings, this converts the fields back to the right type
#  mutate {
#    	convert => ["first_reply_time_in_minutes", "integer"]
#    	convert => ["full_resolution_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["first_resolution_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["agent_wait_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["on_hold_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["full_resolution_time_in_minutes", "integer"]
#    	convert => ["first_resolution_time_in_minutes", "integer"]
#    	convert => ["agent_wait_time_in_minutes", "integer"]
#    	convert => ["requester_wait_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["first_reply_time_in_minutes_within_business_hours", "integer"]
#    	convert => ["on_hold_time_in_minutes", "integer"]
#    	convert => ["requester_wait_time_in_minutes", "integer"]
#  } 
#  
#}
#
#output 
#{
#	elasticsearch
#	{
#		host => "localhost"
#		port => "9200"
#		index => "zendesk"
#		# Referenced in plugin code. Do not change.
#		index_type => "%{es_doc_type}"
#		# Referenced in plugin code. Do not change.
#		protocol => "http"
#		document_id => "%{es_doc_id}"
#       # Set your own index template (eg. zendesk_template) to Elasticsearch using the rest api and 
#       # reference the template_name in the elasticsearch output.
#		manage_template => false
#		template_name => zendesk_template	
#	}
#}
 

class LogStash::Inputs::Zendesk < LogStash::Inputs::Base
  config_name "zendesk"
  milestone 1

  default :codec, "json"

  # Zendesk domain.  
  #   Example: 
  #     elasticsearch.zendesk.com
  config :domain, :validate => :string, :required => true

  # Zendesk user with admin role.
  #   Requires a Zendesk admin user account.
  config :username, :validate => :string, :required => true

  ## Zendesk api token
  #   Ask your Zendesk administrator for your company's api token.
  config :api_token, :validate => :password, :required => false

  # If set to true, enable additional logging from Zendesk client api.
  config :api_logger, :validate => :boolean, :required => false, :default => false

  # Whether or not to fetch organizations.
  config :fetch_organizations, :validate => :boolean, :required => false, :default => true

  # Whether or not to fetch users.
  config :fetch_users, :validate => :boolean, :required => false, :default => true

  # Whether or not to fetch tickets.
  config :fetch_tickets, :validate => :boolean, :required => false, :default => true

  # This is the criteria for fetching tickets in the form of last updated N days ago.
  # Updated tickets include new tickets.
  #   Examples:
  #     0.5 = updated in the past 12 hours
  #     1  = updated in the past day
  #     7 = updated in the past week
  #     -1 = get all tickets (when this mode is chosen, the plugin will run only once, not continuously)
  config :fetch_tickets_last_updated_n_days_ago, :validate => :number, :required => false, :default => 1

  # Whether or not to fetch ticket comments (certainly, you can only fetch comments if you are fetching tickets).
  config :fetch_comments, :validate => :boolean, :required => false, :default => false

  # This is added for enterprise search use cases.  Enabling this will add comments to each ticket document created.
  # This option requires fetch_comments => true.
  config :append_comments_to_tickets, :validate => :boolean, :required => false, :default => false

  # Creates a Zendesk client.
  public
  def register
    require 'zendesk_api' # 1.3.5
    require 'active_support/all'
    @organizations = Hash.new
    @users = Hash.new
    zendesk_client
  end # end register

  # Zendesk client.  Uses api token to initiate a Zendesk client.
  def zendesk_client
  
    @logger.info("Creating a Zendesk client", :username => @username, :api_version => "https://#{@domain}/api/v2")
    @zd_client = ZendeskAPI::Client.new do |zconfig|
      zconfig.url = "https://#{@domain}/api/v2" # zendesk ruby client api 1.3.5 supports zendesk api v2
      zconfig.username = @username
      zconfig.token = @api_token.value
      zconfig.retry = true # this is a feature of the Zendesk client api, it automatically retries when hitting rate limits
      if @api_logger
        zconfig.logger = Logger.new(STDOUT)
      end
    end

    # Zendesk automatically switches to anonymous user login when credentials are invalid.
    # When this happens, no data will be fetched. This is added to prevent further execution due to invalid credentials.
    if @zd_client.current_user.instance_variable_get("@attributes").id.nil?
      raise RuntimeError.new("Cannot initialize a valid Zendesk client.  Please check your login credentials.")
    else
      @logger.info("Successfully initialized a Zendesk client", :username => @username)
    end  
    
  end

# Get organizations. Organizations will be indexed using "organization" type.
  private
  def get_organizations(output_queue)
    begin
      @logger.info("Processing organizations ...")
      puts "Processing organizations ..."
      orgs = @zd_client.organizations
      @logger.info("Number of organizations", :count => orgs.count.to_s)

      count = 0
      orgs.all do |org|

        count = count + 1
        @logger.info("Organization", :name => org.name, :progress => "#{count.to_s}/#{orgs.count.to_s}")

        event = LogStash::Event.new()
        org.instance_variable_get("@attributes").each do |k,v|
          event[k] = v
        end # end attrs
        event["es_doc_type"] = "organization"
        event["es_doc_id"] = org.id
        decorate(event)
        output_queue << event

        @logger.info("Done processing organization", :name => org.name)
        @organizations[org.id] = org
      end # end orgs loop 
    rescue => e
      @logger.error(e.message, :method => "get_organizations")
    end
      orgs = nil
        @logger.info("Done processing organizations.")
  end # end get_organizations

# Get users.  Users will be indexed using "user" type.  
  private
  def get_users(output_queue)
    begin
      @logger.info("Processing users ...")
      puts "Processing users ..."
      users = @zd_client.users
      @logger.info("Number of users", :count => users.count.to_s)

      count = 0
      users.all do |user|

        count = count + 1
        @logger.info("User", :name => user.name, :progress => "#{count.to_s}/#{users.count.to_s}")

        event = LogStash::Event.new()
        user.instance_variable_get("@attributes").each do |k,v|
          if k == "user_fields"
            v.each do |ik,iv|
              event[ik] = iv
            end
          else
            event[k] = v
          end # end user fields
        end # end attrs
        event["es_doc_type"] = "user"
        event["es_doc_id"] = user.id
        # Pull organization name into user object for reporting purposes
        if @fetch_organizations && !@organizations[user.organization_id].nil?
        	event["organization_name"] = @organizations[user.organization_id].name
        end
        decorate(event)
        output_queue << event

        @logger.info("Done processing user", :name => user.name)
        @users[user.id] = user
      end # end user loop 
    rescue => e
      @logger.error(e.message, :method => "get_users")
    end
    users = nil
        @logger.info("Done processing users.")
  end # end get_users

# Get tickets.  Tickets will be indexed using "ticket" type.
# This input uses the Zendesk incremental export api (http://developer.zendesk.com/documentation/rest_api/ticket_export.html) 
# to retrieve tickets due to Zendesk ticket archiving policies 
# (https://support.zendesk.com/entries/28452998-Ticket-Archiving) - so that 
# tickets closed > 120 days ago can be fetched.
  private
  def get_tickets(output_queue, last_updated_n_days, get_comments)

    # Pull in ticket field names because Zendesk incremental export api 
    # returns unfriendly field_<field_id> for custom ticket field names 
    begin
      @ticketfields = Hash.new
      ticket_fields = @zd_client.ticket_fields
      ticket_fields.each do |tf|
        @ticketfields["field_#{tf.id}"] = tf.title.downcase.gsub(' ','_')
      end

      @logger.info("Processing tickets ...")
      puts "Processing tickets ..."
      next_page = true

      if last_updated_n_days != -1
        start_time = @fetch_tickets_last_updated_n_days_ago.day.ago.to_i
      else
        start_time = 0
      end

      tickets = ZendeskAPI::Ticket.incremental_export(@zd_client, start_time)

      next_page_from_each = String.new
      next_page_from_next = String.new
      count = 0

      while next_page && tickets.count > 0
		@logger.info("Next page from Zendesk api", :next_page_url => tickets.instance_variable_get("@next_page"))
        @logger.info("Number of tickets returned from current incremental export page request", :count => tickets.count.to_s)
        tickets.each do |ticket|
          next_page_from_each = tickets.instance_variable_get("@next_page")

          if ticket.status=='Deleted'
            # Do nothing, previously deleted tickets will show up in incremental export, but does not make sense to fetch
            @logger.info("Skipping previously deleted ticket", :ticket_id => ticket.id.to_s)
          else
            count = count + 1
            @logger.info("Ticket", :id => ticket.id.to_s, :progress => "#{count.to_s}/#{tickets.count.to_s}")
            process_ticket(output_queue,ticket,get_comments)
            @logger.info("Done processing ticket", :id => ticket.id.to_s)
          end #end Deleted status check
        end # end ticket loop
        tickets.next
        next_page_from_next = tickets.instance_variable_get("@next_page")
        # Zendesk api creates a next page attribute in its incremental export response including a generated
        # start_time for the "next page" request.
        # Occasionally (potential bug in Zendesk api), it generates the next page request with the same start_time as the originating request.
        # When this happens, it will keep requesting the same page over and over again.  Added a check to workaround this
        # undesirable behavior.
        if next_page_from_next == next_page_from_each
          next_page = false
        end

        count = 0

      end # end while
        # Zendesk api generates the start_time for the next page request.
        # If it ends up generating a start time that is within 5 minutes from now, it will return the following message
        # instead of a regular json response:
        # "Too recent start_time. Use a start_time older than 5 minutes". 
        # This is added to ignore the message and treat it as benign.
    rescue => e
      if e.message.index 'Too recent start_time'
		# Do nothing for "Too recent start_time. Use a start_time older than 5 minutes" message returned by Zendesk api
		# This simply means that there are no more pages to fetch
        next_page = false
      else
      	@logger.error(e.message, :method => "get_tickets")
      end
    end

    @ticketfields = nil
    tickets = nil
    @logger.info("Done processing tickets.")

  end # end get_tickets

# Index each ticket fetched back.  Process comments if get_comments => true.
  private
  def process_ticket(output_queue,ticket,get_comments)
    begin


      event = LogStash::Event.new()
      ticket.instance_variable_get("@attributes").each do |k,v|
        # Zendesk incremental export api returns all integer and date fields as strings.
        # It also returns unfriendly field names for ticket fields (eg. field_<num>).
        # String to integer conversion is handled via Logstash filters.  String to date conversion is
        # handled here.  Also pulling in actual custom ticket field names.
        if k.match(/^field_/) && !@ticketfields[k].nil?
          event[@ticketfields[k]] = v
        elsif k.match(/_at$/)
          event[k] = v.nil? ? nil : Time.parse(v).iso8601
        else
          event[k] = v
        end
      end # end ticket fields

      event["es_doc_type"] = "ticket"
      event["es_doc_id"] = ticket.id
      if get_comments
        event["comments"] = get_ticket_comments(output_queue,ticket,@append_comments_to_tickets)
        # This is commented out for future use when Kibana supports the display of array of objects
        #event["comments"] = get_ticket_comments_future(output_queue,ticket,@append_comments_to_tickets)  
      end
      decorate(event)
      output_queue << event
    rescue => e
      @logger.error(e.message, :method => "process_ticket")
    end

  end # end process tickets


# Get ticket comments.  Comments will be indexed using the "comment" type.
# If append_comments => true, create a single large text field bundling up related comments (sorted descendingly) 
# and append to parent ticket when indexing tickets.
# Also pull in several ticket, user and organization fields that may be useful for reporting such as author name, etc..
# Note:  Use get_ticket_comments_future (commented out) when Kibana handles array of objects in the future instead of
# appending comments to a single large text field.
  private
  def get_ticket_comments(output_queue,ticket,append_comments)

    begin
      all_comments = String.new
      comment_arr = Array.new

      comments = @zd_client.tickets.find(id: ticket.id).comments
      @logger.info("Processing comments", :ticket_id => ticket.id.to_s, :number_of_comments => comments.count().to_s)

      count = 0
      comments.all do |comment|
        count = count + 1
        @logger.info("Comment", :id => comment.id.to_s, :progress => "#{count.to_s}/#{comments.count.to_s}")
        if append_comments # append comments as a single large text field to ticket
          @logger.info("Appending comment to ticket", :comment_id => comment.id.to_s, :ticket_id => ticket.id.to_s)
          tmp_comments = String.new
          tmp_comments << "---------------------------------------------------\n"
          tmp_comments << "Public: #{comment.public.to_s}\n"
          author_name = @fetch_users && !@users[comment.author_id].nil? ? @users[comment.author_id].name : nil
          tmp_comments << "Author: #{author_name}\n"
          tmp_comments << "Created At: #{comment.created_at.to_s}\n\n"
          tmp_comments << "#{comment.body}\n"
          comment_arr << "#{tmp_comments}\n"
          tmp_comments = nil
        end

        event = LogStash::Event.new()
        comment.instance_variable_get("@attributes").each do |k,v|
          event[k] = v
        end # end attrs

        event["ticket_subject"] = ticket.subject
        event["ticket_id"] = ticket.id
        event["ticket_organization_name"] = ticket.organization_name
        if (@fetch_users && !@users[comment.author_id].nil?) 
        	event["author_name"] = @users[comment.author_id].name
        end
        event["es_doc_type"] = "comment"
        event["es_doc_id"] = comment.id
        decorate(event)
        output_queue << event
        @logger.info("Done processing comment", :id => comment.id.to_s)
      end #end comment loop

    rescue => e
        @logger.error(e.message, :method => "get_ticket_comments")
    end

    if comment_arr.count() > 0
      comment_arr.reverse.each { |x|
        all_comments << x.to_s
      }
      comment_arr = nil
      comments = nil
        @logger.info("Done processing comments.")
      all_comments
    end

  end


# For future use:  Same as get_ticket_comments, except that comments are appended to tickets
# as an array of comment objects instead of a single large text field.
# This is commented out until Kibana provides a panel that handles array of objects for display
=begin
  private
  def get_ticket_comments_future(output_queue,ticket,append_comments)

    begin
      comment_arr = Array.new

      comments = @zd_client.tickets.find(id: ticket.id).comments
      @logger.info("Processing comments", :ticket_id => ticket.id.to_s, :number_of_comments => comments.count().to_s)

      count = 0
      comments.all do |comment|
        count = count + 1
        @logger.info("Comment", :id => comment.id.to_s, :progress => "#{count.to_s}/#{comments.count.to_s}")

        if append_comments # append comments as a single large text field to ticket
          @logger.info("Appending comment to ticket", :comment_id => comment.id.to_s, :ticket_id => ticket.id.to_s)
          comment_arr << comment.instance_variable_get("@attributes")
        end

        event = LogStash::Event.new()
        comment.instance_variable_get("@attributes").each do |k,v|
          event[k] = v
        end # end attrs

        if ((!comment.metadata.system.longitude.nil?) && (!comment.metadata.system.latitude.nil?))
        geo_json = Array.new
        geo_json[0] = comment.metadata.system.longitude
        geo_json[1] = comment.metadata.system.latitude
        event["geocode"] = geo_json
        end
        event["ticket_subject"] = ticket.subject
        event["ticket_id"] = ticket.id
        event["ticket_organization_name"] = ticket.organization_name
        if (@fetch_users && !@users[comment.author_id].nil?) 
        	event["author_name"] = @users[comment.author_id].name
        end
        event["es_doc_type"] = "comment"
        event["es_doc_id"] = comment.id
        decorate(event)
        output_queue << event
        @logger.info("Done processing comment", :id => comment.id.to_s)
        end #end comments loop

        rescue => e
        @logger.error(e.message, :method => "get_ticket_comments_future")
        end
        comments = nil
        @logger.info("Done processing comments.")
        comment_arr
        end
=end

private
def sleep_countdown(m)
  while m > 0
    puts "#{m}..."
    sleep(60)
    m = m - 1
  end
end

# Run Zendesk input continuously at 5-min intervals.  If fetch_tickets => true and fetch_tickets_last_updated_n_days_ago => -1,
# run only once to perform a full fetch of all tickets from Zendesk.
        public
        def run(output_queue)
               
          loop do
            start = Time.now
            @logger.info("Starting Zendesk input run.", :start_time => start)
            puts("Starting Zendesk input run at #{start}.")
            @fetch_organizations ? get_organizations(output_queue) : nil
            @fetch_users ? get_users(output_queue) : nil
            @fetch_tickets ? get_tickets(output_queue, @fetch_tickets_last_updated_n_days_ago, @fetch_comments) : nil
            @logger.info("Completed in (minutes).", :duration => ((Time.now - start)/60).round(2))
            puts "Completed in #{((Time.now - start)/60).round(2)} minutes."
            if @fetch_tickets_last_updated_n_days_ago == -1
            	break
            end
            @logger.info("Sleeping before next run ...", :minutes => 5)
            puts "Sleeping 5 minutes before next run ..."
            sleep_countdown(5)
          end # end loop

		rescue LogStash::ShutdownSignal
			@organizations = nil
			@users = nil
			@zd_client = nil

        end # def run

        end # class LogStash::Inputs::Elasticsearch
