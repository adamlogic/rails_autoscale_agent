module RailsAutoscaleAgent
  class Request
    attr_reader :id, :entered_queue_at, :path, :method, :size

    def initialize(env, config)
      @config = config
      @id = env['HTTP_X_REQUEST_ID']
      @path = env['PATH_INFO']
      @method = env['REQUEST_METHOD'].downcase
      @size = env['rack.input'].respond_to?(:size) ? env['rack.input'].size : 0

      if unix_millis = env['HTTP_X_REQUEST_START']
        @entered_queue_at = Time.at(unix_millis.to_f / 1000)
      end
    end

    def ignore?
      @config.ignore_large_requests? && @size > @config.max_request_size
    end

  end
end
