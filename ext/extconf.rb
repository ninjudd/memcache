# This file was derived from Evan's Weaver memcached library: http://github.com/fauna/memcached
# See the ext/LICENSE_AFL3 file.

require 'mkmf'
require 'rbconfig'

HERE        = File.expand_path(File.dirname(__FILE__))
BUNDLE      = Dir.glob("libmemcached-*.tar.gz").first
BUNDLE_PATH = BUNDLE.sub(".tar.gz", "")

$CXXFLAGS = " -std=gnu++98 -fPIC"

if !ENV["EXTERNAL_LIB"]
  $includes    = " -I#{HERE}/include"
  $libraries   = " -L#{HERE}/lib"
  $CFLAGS      = "#{$includes} #{$libraries} #{$CFLAGS}"
  $LDFLAGS     = "#{$libraries} #{$LDFLAGS}"
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
        puts(cmd = "./configure --prefix=#{HERE} --without-memcached --disable-shared --disable-dependency-tracking #{ARGV.join(' ')} 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)

        puts(cmd = "make CXXFLAGS='#{$CXXFLAGS}' || true 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)

        puts(cmd = "make install || true 2>&1")
        raise "'#{cmd}' failed" unless system(cmd)
      end

      system("rm -rf #{BUNDLE_PATH}") unless ENV['DEBUG'] or ENV['DEV']
    end
  end
  
  # Absolutely prevent the linker from picking up any other libmemcached
  if File.exists?("#{HERE}/lib/amd64/libmemcached.a")
    # fix linking issue under solaris
    # https://github.com/ninjudd/memcache/issues/5
    Dir.chdir("#{HERE}/lib/amd64") do
      system('cp -f libmemcached.a  ../libmemcached_gem.a')
      system('cp -f libmemcached.la ../libmemcached_gem.la')
    end
  else
    Dir.chdir("#{HERE}/lib") do
      system('cp -f libmemcached.a  libmemcached_gem.a')
      system('cp -f libmemcached.la libmemcached_gem.la')
    end
  end
  
  $LIBS << " -lmemcached_gem"
end

# ------------------------------------------------------
# thanks to: https://gist.github.com/IanVaughan/5489431
$CPPFLAGS += " -DRUBY_19" if RUBY_VERSION =~ /1.9/
$CPPFLAGS += " -DRUBY_20" if RUBY_VERSION =~ /2.0/
 
puts "*** Using Ruby version: #{RUBY_VERSION}"
puts "*** with CPPFLAGS: #{$CPPFLAGS}"
# ------------------------------------------------------
create_makefile('memcache/native_server')
