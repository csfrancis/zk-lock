require 'test/unit'
require 'zklock'

class ZKLock::LockTest < Test::Unit::TestCase
  SLEEP_TIMEOUT = 0.5

  def setup
    @server = "localhost:2181"
    @path = "/zklock/lock"
    @c = ZKLock::Connection.new(@server)
  end

  def teardown
    @c.close if @c.connected?
  end

  def test_create_lock_fails_with_no_argument
    assert_raise ArgumentError do
      ZKLock::SharedLock.new
    end
  end

  def test_create_shared_lock
    ZKLock::SharedLock.new(@path, @c)
  end

  def test_shared_lock_is_unlocked
    l = ZKLock::SharedLock.new(@path, @c)
    refute l.locked?
  end

  def test_unlocked_unlocked_lock_returns_false
    l = ZKLock::SharedLock.new(@path, @c)
    refute l.unlock
  end

  def test_create_shared_lock_lock_unlock
    l = ZKLock::SharedLock.new(@path, @c)
    assert l.lock
    assert l.locked?
    l.unlock :timeout => -1
    refute l.locked?
  end

  def test_create_exclusive_lock_lock_unlock
    l = ZKLock::ExclusiveLock.new(@path, @c)
    assert l.lock
    assert l.locked?
    l.unlock :timeout => -1
    refute l.locked?
  end

  def test_create_shared_lock_lock_twice_raises
    l = ZKLock::SharedLock.new(@path, @c)
    assert l.lock
    assert_raise ZKLock::Exception do
      assert l.lock
    end
    l.unlock
  end

  def test_create_shared_lock_lock_invalid_server_raises
    c = ZKLock::Connection.new("localhost:21181")
    l = ZKLock::SharedLock.new(@path, c)
    assert_raise ZKLock::Exception do
      l.lock
    end
  end

  def test_exclusive_lock_blocks_when_shared_locked
    mon = Monitor.new
    cond = mon.new_cond
    in_shared_lock = false

    Thread.new do
      begin
        s = ZKLock::SharedLock.new(@path, @c)
        s.lock(:blocking => true)
        in_shared_lock = true
        mon.synchronize do
          cond.signal
        end
        sleep(SLEEP_TIMEOUT)
        s.unlock
        mon.synchronize do
          in_shared_lock = false
          cond.signal
        end
      rescue Exception => e
        puts e.inspect
      end
    end

    mon.synchronize do
      cond.wait
    end

    e = ZKLock::ExclusiveLock.new(@path, @c)
    assert in_shared_lock
    start_time = Time.now
    e.lock(:blocking => true)
    assert Time.now - start_time > SLEEP_TIMEOUT
    if in_shared_lock
      mon.synchronize do
        break unless in_shared_lock
        cond.wait
      end
    end
    refute in_shared_lock
    e.unlock
  end

  def test_shared_lock_blocks_when_exclusive_locked
    mon = Monitor.new
    cond = mon.new_cond
    in_exclusive_lock = false

    Thread.new do
      begin
        e = ZKLock::ExclusiveLock.new(@path, @c)
        e.lock(:blocking => true)
        in_exclusive_lock = true
        mon.synchronize do
          cond.signal
        end
        sleep(SLEEP_TIMEOUT)
        e.unlock
        mon.synchronize do
          in_exclusive_lock = false
          cond.signal
        end
      rescue Exception => e
        puts e.inspect
      end
    end

    mon.synchronize do
      cond.wait
    end

    s = ZKLock::SharedLock.new(@path, @c)
    assert in_exclusive_lock
    start_time = Time.now
    s.lock(:blocking => true)
    assert Time.now - start_time > SLEEP_TIMEOUT
    if in_exclusive_lock
      mon.synchronize do
        break unless in_exclusive_lock
        cond.wait
      end
    end
    refute in_exclusive_lock
    s.unlock
  end

  def test_shared_lock_returns_false_when_exclusive_locked
    mon = Monitor.new
    cond = mon.new_cond
    in_exclusive_lock = false

    Thread.new do
      begin
        e = ZKLock::ExclusiveLock.new(@path, @c)
        e.lock(:blocking => true)
        in_exclusive_lock = true
        mon.synchronize do
          cond.signal
        end
        sleep(SLEEP_TIMEOUT)
        e.unlock
        mon.synchronize do
          in_exclusive_lock = false
          cond.signal
        end
      rescue Exception => e
        puts e.inspect
      end
    end

    mon.synchronize do
      cond.wait
    end

    s = ZKLock::SharedLock.new(@path, @c)
    assert in_exclusive_lock
    start_time = Time.now
    refute s.lock
    if in_exclusive_lock
      mon.synchronize do
        break unless in_exclusive_lock
        cond.wait
      end
    end
    refute in_exclusive_lock
  end

  def test_with_lock_yields
    yielded = false
    l = ZKLock::SharedLock.new(@path, @c)
    l.with_lock do
      yielded = true
    end
    refute l.locked?
    assert yielded
  end

  def test_with_lock_raises_with_no_block
    l = ZKLock::SharedLock.new(@path, @c)
    assert_raise ArgumentError do
      l.with_lock
    end
  end
end
