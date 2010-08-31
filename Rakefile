require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "memcache"
    gem.summary = %Q{Advanced ruby memcache client}
    gem.description = %Q{Ruby client for memcached supporting advanced protocol features and pluggable architecture.}
    gem.email = "code@justinbalthrop.com"
    gem.homepage = "http://github.com/ninjudd/memcache"
    gem.authors = ["Justin Balthrop"]
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/*_test.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

task :clean do
  `rm -rf ext/lib ext/bin ext/share ext/include`
end

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  if File.exist?('VERSION')
    version = File.read('VERSION')
  else
    version = ""
  end

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "memcache #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

namespace :test => 'lib/memcache/native_server.o' do
  Rake::TestTask.new(:native) do |t|
    t.libs << 'test'
    t.pattern = 'test/memcache_*native_server_test.rb'
    t.verbose
  end
end

file 'lib/memcache/native_server.o' do
  `cd ext && ruby extconf.rb && make && cp native_server.bundle native_server.o ../lib/memcache/`
end
