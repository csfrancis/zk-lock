require 'rake/testtask'

namespace :build do
  task :clean do
    cd 'ext' do
      sh 'rake clean'
    end

    Rake::Task['build'].invoke
  end

  task :clobber do
    cd 'ext' do
      sh 'rake clobber'
    end

    Rake::Task['build'].invoke
  end
end

desc "Build C component"
task :build do
  cd 'ext' do
    sh "rake"
  end
end

Rake::TestTask.new(:test => :build) do |t|
  t.libs << "test"
  t.test_files = FileList['test/**/*_test.rb']
  t.verbose = true
end

task :clean    => 'build:clean'
task :clobber  => 'build:clobber'
task :default  => :test
