# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket" # for Socket.gethostname

# Read events from a Java NIO.2 Path.
#
# By default, each event is assumed to be one line. If you
# want to join lines, you'll want to use the multiline filter.
class LogStash::Inputs::Nio2Path < LogStash::Inputs::Base
  config_name "nio2path"
  milestone 1

  default :codec, "line"
  
  # A path glob for monitored files
  # 
  # The directory must be exact, but the filename can be a pattern.
  # This can not be a recursive glob (** is not supported).
  #
  # Example: schema://var/log/apache/*.log
  config :path, :validate => :string, :required => true
  
  # Choose where Logstash starts initially reading files - at the beginning or
  # at the end. The default behavior treats files like live streams and thus
  # starts at the end. If you have old data you want to import, set this
  # to 'beginning'
  config :start_position, :validate => [ "beginning", "end"], :default => "end"
  
  # Maximum sleep time for polling filesystem changes (milliseconds)
  config :timeout, :validate => :number, :default => 100

  public
  def register
    @host = Socket.gethostname
    @streams = {}
    parsePath
    openPath
    scanPath
    setupMatcher
  end

  def parsePath
    schemaslash = @path.rindex('://')
    lastslash = @path.rindex('/')
    if !schemaslash
      if lastslash
        # foo/ OR /foo OR foo/bar OR file:/...
        @dirname = @path[0, lastslash + 1]
        if @path.end_with?('/')
          # ...foo/bar/
          @basename = "*"
        else
          # ...foo/bar
          @basename = @path[lastslash + 1, @path.length - lastslash - 1]
        end
      else
        # foo
        @dirname = Dir.getwd
        @basename = @path
      end
    elsif lastslash == schemaslash + 2
      # schema://foo:bar
      @dirname = @path + "/"
      @basename = "*"
    elsif @path.end_with?('/')
      # schema://foo:bar/baz/
      @dirname = @path
      @basename = "*"
    else
      # schema://foo:bar/baz
      @dirname = @path[0, lastslash + 1]
      @basename = @path[lastslash + 1, @path.length - lastslash - 1]
    end
  end

  def openPath
    begin
      uri = java.net.URI.new(@dirname)
      @javapath = java.nio.file.Paths.get(uri)
    rescue java.lang.IllegalArgumentException => e
      @javapath = java.nio.file.Paths.get(@dirname)
    rescue java.lang.Exception => e
      @logger.warn("Unable to open path", :dirname => @dirname, :error => e)
      raise e
    end
  end

  def scanPath
    begin
      # Get list of files in path using glob
      java.nio.file.Files.newDirectoryStream(@javapath, @basename).each do |file|
        next if file.getFileName.toString.eql?("..")
        # Save a reader for each file (non-directory)
        if (!java.nio.file.Files.isDirectory(file))
          @streams[file.getFileName] = java.nio.file.Files.newInputStream(file)
        end
      end
      # Register a filesystem watchservice to monitor changes to files in the path
      @watchservice = @javapath.getFileSystem.newWatchService
    rescue java.lang.Exception => e
      @logger.error("Error looking up file", :dirname => @dirname, :error => e)
      raise e
    end
    
    begin
      @javapath.register(@watchservice, java.nio.file.StandardWatchEventKinds::ENTRY_MODIFY,
                         java.nio.file.StandardWatchEventKinds::ENTRY_CREATE,
                         java.nio.file.StandardWatchEventKinds::ENTRY_DELETE)
    rescue java.lang.Exception => e
      @logger.warn("Unable to register watcher for a path", :path => @javapath, :error => e)
    end
  end

  def setupMatcher
    filesystem = @javapath.getFileSystem
    begin
      @matcher = filesystem.getPathMatcher("glob:" + @basename)
    rescue java.lang.Exception => e
      @logger.warn("Unable to get glob matcher", :filesystem => filesystem, :path => @basename, :error => e)
      raise e
    end
  end

  def run(queue)
    # Read or skip data in existing files
    processBeginning(queue)
    
    # Watch for new & modified files
    monitorFiles(queue) # while true
    finished
  end

  def processBeginning(queue)
    @streams.each do |file, str|
      absolutepath = @javapath.resolve(file).toAbsolutePath
      begin
        if (@start_position == "end")
          str.skip(java.nio.file.Files.size(absolutepath))
        else
          @codec.decode(readMore(str)) do |event|
            decorate(event)
            event["host"] ||= @host
            event["path"] ||= absolutepath.toString
            queue << event
          end
        end
      rescue java.lang.Exception => e
        @logger.warn("Unable to read from a file", :file => file, :error => e)
      end
    end
  end

  def monitorFiles(queue)
    while true
      begin
        # Get new filesystem change events, or sleep
        while (watchkey = @watchservice.poll(@timeout, java.util.concurrent.TimeUnit::MILLISECONDS))
          begin
            watchkey.pollEvents.each do |watchevent|
              p = watchevent.context
              if (@matcher.matches(p))
                file = @javapath.resolve(p).toAbsolutePath

                # If file has been deleted, clear out its reader
                if (watchevent.kind.name == java.nio.file.StandardWatchEventKinds::ENTRY_DELETE.name)
                  stream = @streams[p]
                  if (stream)
                    stream.close
                  end
                  @streams[p] = nil

                  # Otherwise, file has been created or modified
                elsif (!java.nio.file.Files.isDirectory(file))

                  # Look for an existing reader for the new or modified file
                  stream = @streams[p]

                  # If this file was just created, set up a reader for it
                  if (stream.nil?)
                    stream = java.nio.file.Files.newInputStream(file)
                    @streams[p] = stream
                  end

                  # Read new lines from file
                  @codec.decode(readMore(stream)) do |event|
                    decorate(event)
                    event["host"] ||= @host
                    event["path"] ||= file.toString
                    queue << event
                  end
                end
              end
            end
            watchkey.reset
          rescue java.lang.Exception => e
            @logger.warn("Unknown error", :error => e)
            raise e
          end

        end
      rescue EOFError, LogStash::ShutdownSignal
        break
      end
    end
  end

  # def run

  private
  def readMore(stream)
    buffer = java.nio.ByteBuffer.allocate(stream.available)
    stream.read(buffer)
    return org.jruby.RubyString.bytesToString(buffer.array)
  end
  
  public
  def teardown
    @watchservice.close
    @logger.debug("nio2path shutting down.")
    finished
  end # def teardown
end # class LogStash::Inputs::Nio2Path
