class Fluent::HTTPOutput < Fluent::Output
  Fluent::Plugin.register_output('http', self)

  def initialize
    super
    require 'net/http'
    require 'uri'
    require 'yajl'
  end

  # Endpoint URL ex. http://localhost.local/api/
  config_param :endpoint_url, :string

  # Set Net::HTTP.verify_mode to `OpenSSL::SSL::VERIFY_NONE`
  config_param :ssl_no_verify, :bool, :default => false

  # HTTP method
  config_param :http_method, :string, :default => :post

  # form | json
  config_param :serializer, :string, :default => :form

  # Simple rate limiting: ignore any records within `rate_limit_msec`
  # since the last one.
  config_param :rate_limit_msec, :integer, :default => 0

  # Raise errors that were rescued during HTTP requests?
  config_param :raise_on_error, :bool, :default => true

  # nil | 'none' | 'basic'
  config_param :authentication, :string, :default => nil
  config_param :username, :string, :default => ''
  config_param :password, :string, :default => '', :secret => true

  config_param :reuse_limit, :integer, default: 1000

  def configure(conf)
    super

    @ssl_verify_mode = if @ssl_no_verify
                         OpenSSL::SSL::VERIFY_NONE
                       else
                         OpenSSL::SSL::VERIFY_PEER
                       end

    serializers = [:json, :form]
    @serializer = if serializers.include? @serializer.intern
                    @serializer.intern
                  else
                    :form
                  end

    http_methods = [:get, :put, :post, :delete]
    @http_method = if http_methods.include? @http_method.intern
                    @http_method.intern
                  else
                    :post
                  end

    @auth = case @authentication
            when 'basic' then :basic
            else
              :none
            end

    @connection_count = 0

    @last_request_time = nil
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format_url
    @endpoint_url
  end

  def format_uri
    @format_uri ||= URI.parse(format_url)
  end

  def set_body(req, _tag, _time, record)
    if @serializer == :json
      set_json_body(req, record)
    else
      req.set_form_data(record)
    end
    req
  end

  def set_header(req, _tag, _time, _record)
    req
  end

  def set_json_body(req, data)
    req.body = Yajl.dump(data)
    req['Content-Type'] = 'application/json'
  end

  def create_request(tag, time, record)
    req = Net::HTTP.const_get(@http_method.to_s.capitalize).new(format_uri.path)
    set_body(req, tag, time, record)
    set_header(req, tag, time, record)
    return req
  end

  def connection
    if @connection
      if @connection_count > @reuse_limit
        @connection.finish
        create_connection
      else
        @connection
      end
    else
      create_connection
    end
  end

  def create_connection
    @connection = Net::HTTP.start format_uri.host, format_uri.port
    if format_uri.scheme == 'https'
      @connection.use_ssl = true
      @connection.ssl_verify_mode = @ssl_verify_mode
    end
    @connection
  end

  def send_request(req)
    is_rate_limited = (@rate_limit_msec != 0 and not @last_request_time.nil?)
    if is_rate_limited and ((Time.now.to_f - @last_request_time) * 1000.0 < @rate_limit_msec)
      $log.info('Dropped request due to rate limiting')
      return
    end

    res = nil

    begin
      if @auth and @auth == :basic
        req.basic_auth(@username, @password)
      end
      @last_request_time = Time.now.to_f
      res = connection.request(req)
      @connection_count += 1
    rescue => e # rescue all StandardErrors
      # server didn't respond
      $log.warn "Net::HTTP.#{req.method.capitalize} raises exception: #{e.class}, '#{e.message}'"
      raise e if @raise_on_error
    else
      unless res and res.is_a?(Net::HTTPSuccess)
        res_summary = if res
                        "#{res.code} #{res.message} #{res.body}"
                      else
                        "res=nil"
                      end
        $log.warn "failed to #{req.method} #{uri} (#{res_summary})"
      end #end unless
    end # end begin
  end # end send_request

  def handle_record(tag, time, record)
    req = create_request(tag, time, record)
    send_request(req)
  end

  def emit(tag, es, chain)
    es.each do |time, record|
      handle_record(tag, time, record)
    end
    chain.next
  end
end
