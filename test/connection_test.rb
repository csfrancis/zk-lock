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

  def test_connection_connect
    @c.connect
    sleep(0.5)
    assert @c.connected?
  end

  def test_connection_close
    @c.connect
    sleep(0.5)
    assert @c.connected?
    @c.close
    sleep(0.5)
    assert @c.closed?
  end

  def test_connection_close_raises_when_not_connected
    assert_raise ZKLock::Exception do
      @c.close
    end
  end

  def test_connection_timeout
    @c.connect(:timeout => 0.5)
    assert @c.connected?
    @c.close
  end

  def test_connection_invalid_server
    c = ZKLock::Connection.new("localhost:21181")
    c.connect(:timeout => 0.5)
    refute c.connected?
  end

  def test_connection_connect_twice_raises
    @c.connect
    assert_raise ZKLock::Exception do
     @c.connect
    end
  end
end
