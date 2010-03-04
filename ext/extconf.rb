require 'mkmf'
require 'rbconfig'

# Code taken from Evan's Weaver memcached library: http://github.com/fauna/memcached

HERE        = File.expand_path(File.dirname(__FILE__))
BUNDLE      = Dir.glob("libmemcached-*.tar.gz").first
BUNDLE_PATH = BUNDLE.sub(".tar.gz", "")

$CFLAGS   = "#{RbConfig::CONFIG['CFLAGS']} #{$CFLAGS}".gsub("$(cflags)", "")
$LDFLAGS  = "#{RbConfig::CONFIG['LDFLAGS']} #{$LDFLAGS}".gsub("$(ldflags)", "")
$CXXFLAGS = " -std=gnu++98"
$CPPFLAGS = $ARCH_FLAG = $DLDFLAGS = ""

if ENV['DEBUG']
  puts "Setting debug flags."
  $CFLAGS << " -O0 -ggdb -DHAVE_DEBUG"
  $EXTRA_CONF = " --enable-debug"
end

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
        puts(cmd = "env LDFLAGS='-fPIC #{$LDFLAGS}' ./configure --prefix=#{HERE} --without-memcached --disable-shared --disable-dependency-tracking --disable-utils #{$EXTRA_CONF} 2>&1")
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
  Dir.chdir("#{HERE}/lib") do
    system('cp -f libmemcached.a  libmemcached_gem.a') 
    system('cp -f libmemcached.la libmemcached_gem.la') 
  end

  $LIBS << " -lmemcached_gem"
end

create_makefile('memcache/native_server')
