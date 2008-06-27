Gem::Specification.new do |s|
	s.name = 'memcache-client'
	s.version = '1.5.0.3'
	s.authors = ['Eric Hodel', 'Robert Cottrell', 'Mike Perham', 'Justin Balthrop']
	s.email = 'justin@geni.com'
	s.homepage = 'http://github.com/ninjudd/memcache-client'
	s.summary = 'A Ruby-based memcached client library (with extensions)'
	s.description = s.summary
	s.extensions << 'ext/crc32/extconf.rb'

	s.require_path = 'lib'

	s.files = ['README.txt', 'License.txt', 'History.txt', 'Rakefile', 'lib/memcache.rb', 'lib/memcache_util.rb', 'lib/memcache_extended.rb', 'lib/memcache_mock.rb', 'ext/crc32/crc32.c']
	s.test_files = ['test/test_mem_cache.rb', 'test/test_memcache_extended.rb', 'test/test_memcache_mock.rb']
end
