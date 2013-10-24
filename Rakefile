require 'rake/testtask'
require 'bundler/gem_tasks'

Rake::TestTask.new do |t|
  t.libs = ['lib', 'test']
  t.pattern = 'test/**/*_test.rb'
  t.verbose = false
end

task :test => 'lib/memcache/native_server.o'

file 'lib/memcache/native_server.o' do
  `cd ext && ruby extconf.rb && make && cp native_server.bundle native_server.o native_server.so ../lib/memcache/`
end

task :default => :test
