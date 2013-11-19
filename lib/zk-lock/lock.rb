module ZKLock
  class Lock
    def with_lock(opts = {})
      raise ArgumentError unless block_given?
      opts = opts.merge({:blocking => true})
      lock(opts)
      yield
    ensure
      unlock(:timeout => 0) if locked?
    end
  end
end
