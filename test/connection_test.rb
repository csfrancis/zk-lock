require 'test/unit'
require 'zklock'

class ZKLock::ConnectionTest < Test::Unit::TestCase
  def setup
    @server = "localhost:2181"
    @c = ZKLock::Connection.new(@server)
  end

  def test_new_connection_is_not_connected
    refute @c.connected?
  end

  def test_new_connection_is_closed
    assert @c.closed?
  end

  def test_connection_requires_server
    assert_raise ArgumentError do
      c = ZKLock::Connection.new
    end
  end
end
