require 'mkmf'
have_library('memcached')
create_makefile('native_server')
