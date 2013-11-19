require 'zklock'
require 'optparse'

options = {}
option_parser = OptionParser.new do |opts|
  opts.banner = "Usage: perf.rb [options]"
  opts.on("-s", "--server SERVERS") { |servers| options[:servers] = servers }
  opts.on("-d", "--duration SECONDS", Integer) { |duration| options[:duration] = duration }
  opts.on("-p", "--processes COUNT", Integer) { |count| options[:process_count] = count }
end

option_parser.parse!
unless [:servers, :duration, :process_count].all? { |key| options.include? key }
  puts option_parser
  exit
end
servers, duration, process_count = options.values_at(:servers, :duration, :process_count)
MAX_SHOP_ID = 200_000

def lock_cycle(zk)
  shop_id = rand(MAX_SHOP_ID)
  lock = ZKLock::SharedLock.new("/shopify/shops/#{shop_id}", zk)
  exit 1 unless lock.lock
  exit 1 unless lock.unlock
end

pipes = process_count.times.map { IO.pipe }
readers = pipes.map do |reader, writer|
  pid = fork do
    reader.close
    cycles = 0
    begin
      ZKLock.open(servers) do |zk|
        fin = Time.now + duration
        cycles += 10.times { lock_cycle(zk) } until fin < Time.now
      end
    rescue ZKLock::Exception
      # the ZKLock Lock#unlock is async, and as a result an exception
      # could be raised when we try to close the connection
    end
    writer.write cycles
  end
  writer.close
  reader
end

lock_count = readers.reduce(0) do |memo, reader|
  result = reader.read.to_i
  reader.close
  memo + result
end

if Process.waitall.all? { |ary| ary.last.success? }
  p "Hammered #{lock_count} in #{duration} seconds (#{lock_count.to_f / duration} op/s)"
else
  p "FAILED"
end
