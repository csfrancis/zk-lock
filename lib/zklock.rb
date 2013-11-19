require File.expand_path('../../ext/zklock', __FILE__)
require 'zk-lock/lock'

module ZKLock
  def self.open(server, opts = {})
    { :timeout => -1 }.each { |k,v|  opts[k] = v if opts[k].nil? }
    raise ArgumentError unless block_given?
    c = Connection.new(server)
    c.connect(opts)
    yield
  ensure
    c.close if c.connected?
  end
end
