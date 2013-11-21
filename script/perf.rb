require 'zklock'
require 'optparse'
require 'descriptive_statistics'

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
  lock.lock(:timeout => 0.5)
  lock.unlock
end

pipes = process_count.times.map { IO.pipe }
readers = pipes.map do |reader, writer|
  pid = fork do
    reader.close
    cycles = 0
    timeouts = 0
    lock_times = []
    connect_times = []
    fin = Time.now + duration
    until fin < Time.now do
      begin
        start_time = Time.now
        ZKLock.open(servers, :timeout => 10) do |zk|
          connect_times << (Time.now - start_time)
          10.times do
            begin
              start_time = Time.now
              lock_cycle(zk)
              lock_times << (Time.now - start_time)
              cycles += 1
            rescue ZKLock::TimeoutError
              timeouts += 1
            end
          end
        end
      rescue ZKLock::TimeoutError
        timeouts += 1
      end
    end
    Marshal.dump([cycles, timeouts, lock_times, connect_times], writer)
  end
  writer.close
  reader
end

results = readers.map do |reader|
  result = Marshal.load(reader)
  reader.close
  result
end

lock_count = results.reduce([0, 0]) do |memo, result|
  [memo[0] + result[0], memo[1] + result[1]]
end

lock_times = results.map {|r| r[2]}.flat_map {|v| v}
connect_times = results.map {|r| r[3]}.flat_map {|v| v}

def show_time_stats(times, process_count)
  puts "Total    #{times.size}"
  puts "Average  #{times.mean}"
  puts "Variance #{times.variance}"
  puts "StdDev   #{times.standard_deviation}"
  puts "P(90)    #{times.percentile(90)}"
  puts "P(95)    #{times.percentile(95)}"
  puts "P(99)    #{times.percentile(99)}"
  puts "Longest  #{times.sort[(times.size - process_count... times.size)]}"
  puts "\n"
end

if Process.waitall.all? { |ary| ary.last.success? }
  puts "Hammered #{lock_count[0]} in #{duration} seconds (#{lock_count[0].to_f / duration} op/s)"
  puts "Timouts  #{lock_count[1]}\n\n"
  puts "Lock Times"
  puts "=========="
  show_time_stats(lock_times, process_count)
  puts "Connect Times"
  puts "=========="
  show_time_stats(connect_times, process_count)
else
  puts "FAILED"
end
