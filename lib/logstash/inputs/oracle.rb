# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket"

# Read rows from an Oracle database.
#
# This is most useful in cases where you are logging directly to a table.
# Any tables being watched must have an 'id' column that is monotonically
# increasing.
#
# No table is read by default.
#
# Plugin creates a new table 'SINCE_TABLE' to track watched tables and last read 'id'
#
# ## Example
#
#     > CREATE TABLE weblogs (
#         id INTEGER,
#         ip STRING,
#         request STRING,
#         response INTEGER);
#     > INSERT INTO weblogs (ip, request, response)
#         VALUES ("1.2.3.4", "/index.html", 200);
#
# Then with this logstash config:
#
#     input {
#       oracle {
#         hostname => "127.0.0.1"
#         port => 1521
#         login => "scott"
#         password => "tiger"
#         db_name => "XE"
#         include_tables => ["weblogs"]
#         path_ojdbc_jar => "/absolute_path_to/ojdbc6.jar"
#       }
#     }
#     output {
#       stdout {
#         debug => true
#       }
#     }
#
# Sample output:
#
#     {
#       "host" => "127.0.0.1",
#       "table" => "weblogs",
#       "@version" => "1",
#       "@timestamp" => "2014-10-02T15:29:10.695Z",
#       "ip" => "1.2.3.4",
#       "request" => "/index.html",
#       "response" => 200
#     }
#

class LogStash::Inputs::Oracle < LogStash::Inputs::Base
  config_name "oracle"
  milestone 1

  # The Oracle database configuration elements
  config :hostname,       :validate => :string, :default => 'localhost'
  config :port,           :validate => :number, :default => 1521
  config :login,          :validate => :string, :default => 'scott'
  config :password,       :validate => :string, :default => 'tiger'
  config :db_name,        :validate => :string, :default => 'test'
  config :path_ojdbc_jar, :validate => :string, :required => true

  # Any tables to include by name.
  # By default no table is followed.
  config :include_tables, :validate => :array, :default => []

  # How many rows to fetch at a time from each SELECT call.
  config :batch, :validate => :number, :default => 5

  SINCE_TABLE = :since_table

  public
  def init_placeholder_table(db)
    begin
      @logger.info("SINCE_TABLE=#{SINCE_TABLE}")
      db.create_table SINCE_TABLE do
        String  :t_table
        Integer :place
      end
    rescue
      @logger.debug('since tables already exists')
    end
  end

  public
  def get_placeholder(db, table)
    since = db[SINCE_TABLE]
    x = since.where(:t_table => "#{table}")
    if x.empty?
      init_placeholder(db, table)
      return 0
    else
      @logger.debug("placeholder already exists, it is #{x.get(:place)}")
      return x.get(:place)
    end
  end

  public
  def init_placeholder(db, table)
    @logger.debug("init placeholder for #{table}")
    since = db[SINCE_TABLE]
    since.insert(:t_table => table, :place => 0)
  end

  public
  def update_placeholder(db, table, place)
    @logger.debug("set placeholder to #{place}")
    since = db[SINCE_TABLE]
    since.where(:t_table => table).update(:place => place)
  end

  public
  def get_all_tables(db)
    return include_tables
  end

  public
  def get_n_rows_from_table(db, table, offset, limit)
    return db["SELECT * FROM (SELECT * FROM #{table} WHERE (id > #{offset}) ORDER BY 'id') WHERE ROWNUM <= #{limit}"].map { |row| row }
  end

  public
  def register
    require 'sequel'
    require "#{path_ojdbc_jar}"
    @logger.info('Connecting Oracle database')
    Java::oracle.jdbc.driver.OracleDriver
    @db = Sequel.connect("jdbc:oracle:thin:#{login}/#{password}@#{hostname}:#{port}:#{db_name}")
    @logger.info('Connected to Oracle database')
    @tables = get_all_tables(@db)
    @table_data = {}
    @logger.info("@tables : #{@tables}")
    @tables.each do |table|
      init_placeholder_table(@db)
      last_place = get_placeholder(@db, table)
      @table_data[table] = { :name => table, :place => last_place }
    end
  end # def register

  public
  def run(queue)
    sleep_min = 0.01
    sleep_max = 5
    sleeptime = sleep_min

    begin
      @logger.debug('Tailing Oracle db')
      loop do
        count = 0
        @table_data.each do |k, table|
          table_name = table[:name]
          offset = table[:place]
          @logger.debug("offset is #{offset}", :k => k, :t_table => table_name)
          rows = get_n_rows_from_table(@db, table_name, offset, @batch)
          count += rows.count
          rows.each do |row|
            event = LogStash::Event.new('host' => hostname, 'db' => @db, 'table' => table_name)
            decorate(event)
            # store each column as a field in the event.
            row.each do |column, element|
              next if column == :id
              event[column.to_s] = element
            end
            queue << event
            @table_data[k][:place] = row[:id]
          end
          # Store the last-seen row in the database
          update_placeholder(@db, table_name, @table_data[k][:place])
        end

        if count == 0
          # nothing found in that iteration
          # sleep a bit
          @logger.debug('No new rows. Sleeping.', :time => sleeptime)
          sleeptime = [sleeptime * 2, sleep_max].min
          sleep(sleeptime)
        else
          sleeptime = sleep_min
        end
      end # loop
    end # begin/rescue
  end #run

end # class Logtstash::Inputs::EventLog

