# vim: syntax=Ruby
require 'rubygems'
require 'rake/rdoctask'
require 'spec/rake/spectask'

task :gem do
	sh "gem build memcache-client.gemspec"
end

task :install => [:gem] do
	sh "sudo gem install memcache-client-*.gem"
end

task :build_extensions do
  sh 'cd ext/crc32; ruby extconf.rb; make'
  ['bundle', 'so'].each do |ext|
    filename = "ext/crc32/crc32.#{ext}"
    FileUtils.cp(filename, 'lib') if File.exists?(filename)
  end
end

task :clean_extensions do
  ['Makefile', 'crc32.o', 'crc32.bundle', 'crc32.so'].each do |file|
    filename = "ext/crc32/#{file}"
    File.delete(filename) if File.exists?(filename)
  end

  ['bundle', 'so'].each do |ext|
    filename = "ext/crc32/crc32.#{ext}"
    File.delete(filename) if File.exists?(filename)
  end
end

Spec::Rake::SpecTask.new do |t|
	t.ruby_opts = ['-rtest/unit']
	t.spec_files = FileList['test/test_*.rb']
	t.fail_on_error = true
end
  
Rake::RDocTask.new do |rd|
	rd.main = "README.rdoc"
	rd.rdoc_files.include("README.rdoc", "lib/**/*.rb")
	rd.rdoc_dir = 'doc'
end
