# This file was derived from Evan's Weaver memcached library: http://github.com/fauna/memcached
# See the ext/LICENSE_AFL3 file.

require 'mkmf'
require 'rbconfig'

HERE        = File.expand_path(File.dirname(__FILE__))
BUNDLE      = Dir.glob("libmemcached-*.tar.gz").first
BUNDLE_PATH = BUNDLE.sub(".tar.gz", "")

$CXXFLAGS = " -std=gnu++98 -fPIC"

def copy_gem(gem_dir)
  Dir.chdir("#{HERE}/#{gem_dir}") do
    # try the extensions in order
    ['so', 'dylib', 'dll'].any? do |ext|
      if File.exist?("#{HERE}/#{gem_dir}/libmemcached.#{ext}")
        system("cp -f libmemcached.#{ext} #{HERE}/lib/libmemcached_gem.#{ext}")
      end
    end or raise 'Unknown libmemcached extension'
  end
end

if !ENV["EXTERNAL_LIB"]
  $includes    = " -I#{HERE}/include"
  $libraries   = " -L#{HERE}/lib"
  $CFLAGS      = "#{$includes} #{$libraries} #{ENV['CFLAGS']}"
  $LDFLAGS     = "#{$libraries} #{ENV['LDFLAGS']}"
  $LIBPATH     = ["#{HERE}/lib"]
  $DEFLIBPATH  = []

  Dir.chdir(HERE) do
    if false and File.exist?("lib")
      puts "Libmemcached already built; run 'rake clean' first if you need to rebuild."
    else
      puts "Building libmemcached."
      puts(cmd = "tar xzf #{BUNDLE} 2>&1")
      raise "'#{cmd}' failed" unless system(cmd)

      Dir.chdir(BUNDLE_PATH) do
        puts(cmd = "./configure --prefix=#{HERE} --without-memcached --disable-dependency-tracking #{ARGV.join(' ')} 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)

        puts(cmd = "make CXXFLAGS=\"$CXXFLAGS #{$CXXFLAGS}\" 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)

        puts(cmd = "make install 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)
      end

      system("rm -rf #{BUNDLE_PATH}") unless ENV['DEBUG'] or ENV['DEV']
    end
  end

  # Absolutely prevent the linker from picking up any other libmemcached
  if File.exists?("#{HERE}/lib/amd64/libmemcached.a")
    # fix linking issue under solaris
    # https://github.com/ninjudd/memcache/issues/5
    copy_gem('/lib/amd64')
  else
    copy_gem('lib') 
  end

  $LIBS << " -lmemcached_gem"
end

# ------------------------------------------------------
# thanks to: https://gist.github.com/IanVaughan/5489431
$CPPFLAGS += " -DRUBY_19" if RUBY_VERSION =~ /1.9/
$CPPFLAGS += " -DRUBY_20" if RUBY_VERSION =~ /2.0/
$CPPFLAGS += " -DRUBY_21" if RUBY_VERSION =~ /2.1/
$CPPFLAGS += " -DRUBY_22" if RUBY_VERSION =~ /2.2/

puts "*** Using Ruby version: #{RUBY_VERSION}"
puts "*** with CPPFLAGS: #{$CPPFLAGS}"
# ------------------------------------------------------
create_makefile('memcache/native_server')
