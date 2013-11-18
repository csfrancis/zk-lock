require 'test/unit'
require 'zklock'

class ZKLock::LockTest < Test::Unit::TestCase
  def setup
    @server = "localhost:2181"
    @c = ZKLock::Connection.new(@server)
  end

  def test_create_lock_fails_with_no_argument
    assert_raise ArgumentError do
      ZKLock::SharedLock.new
    end
  end

  def test_create_shared_lock
    ZKLock::SharedLock.new(@c)
  end

  def test_create_shared_lock_lock
    l = ZKLock::SharedLock.new(@c)
    l.lock
  end

  def test_create_shared_lock_lock
    c = ZKLock::Connection.new("localhost:21181")
    l = ZKLock::SharedLock.new(c)
    assert_raise ZKLock::Exception do
      l.lock
    end
  end
end
