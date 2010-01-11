#include "ruby.h"
#include <libmemcached/memcached.h>

VALUE cMemcache;
VALUE cNativeServer;
VALUE sym_host;
VALUE sym_port;

static ID iv_memcache_flags, iv_memcache_cas;

static void ns_free(void *p) {
  memcached_free(p);
}

static VALUE ns_alloc(VALUE klass) {
  memcached_st *ns;
  VALUE obj;

  ns  = memcached_create(NULL);
  obj = Data_Wrap_Struct(klass, 0, ns_free, ns);
  return obj;
}

static VALUE ns_initialize(VALUE self, VALUE opts) {
  memcached_st *mc;
  VALUE hostv, portv;
  char* host;
  int   port;

  Data_Get_Struct(self, memcached_st, mc);
  hostv = rb_hash_aref(opts, sym_host);
  portv = rb_hash_aref(opts, sym_port);
  host  = StringValuePtr(hostv);
  port  = NIL_P(portv) ? MEMCACHED_DEFAULT_PORT : NUM2INT(portv);

  memcached_server_add(mc, host, port);
  return self;
}

static VALUE ns_get(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key_or_keys, cas, keys, results, key, value;
  memcached_return error;
  static memcached_result_st result;
  size_t       num_keys, i;  
  const char** key_strings;
  size_t*      key_lengths;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key_or_keys, &cas);

  keys = TYPE(key_or_keys) == T_ARRAY ? key_or_keys : rb_ary_new4(1, &key_or_keys);

  num_keys = RARRAY_LEN(keys);
  key_strings = (const char**)   malloc(num_keys * sizeof(char *));
  key_lengths = (size_t *) malloc(num_keys * sizeof(size_t));

  for (i = 0; i < RARRAY(keys)->len; i++) {
    key = StringValue(RARRAY(keys)->ptr[i]);    
    key_lengths[i] = RSTRING_LEN(key);
    key_strings[i] = RSTRING_PTR(key);
  }

  memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, RTEST(cas) ? 1 : 0);
  memcached_mget(mc, key_strings, key_lengths, num_keys);
  memcached_result_create(mc, &result);

  if (keys == key_or_keys) results = rb_hash_new();
  
  while (memcached_fetch_result(mc, &result, &error)) {
    key   = rb_str_new(memcached_result_key_value(&result), memcached_result_key_length(&result));
    value = rb_str_new(memcached_result_value(&result),     memcached_result_length(&result));
    rb_ivar_set(value, iv_memcache_flags, INT2NUM(memcached_result_flags(&result)));
    if (RTEST(cas)) rb_ivar_set(value, iv_memcache_cas, INT2NUM(memcached_result_cas(&result)));
    
    if (keys != key_or_keys) return value;
    rb_hash_aset(results, key, value);
  }
  
  return results;
}

VALUE ns_set(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  memcached_return error;
  static memcached_result_st result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key   = StringValue(key);
  value = StringValue(value);

  error = memcached_set(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                        RTEST(expiry) ? NUM2INT(expiry) : 0,
                        RTEST(flags)  ? NUM2INT(flags)  : 0);
  return value;
}

void Init_native_server() {
  sym_host = ID2SYM(rb_intern("host"));
  sym_port = ID2SYM(rb_intern("port"));
  iv_memcache_flags = rb_intern("@memcache_flags");
  iv_memcache_cas   = rb_intern("@memcache_cas");

  cMemcache     = rb_define_class("Memcache", rb_cObject);
  cNativeServer = rb_define_class_under(cMemcache, "NativeServer", rb_cObject);
  rb_define_alloc_func(cNativeServer, ns_alloc);

  rb_define_method(cNativeServer, "initialize", ns_initialize, 1);
  rb_define_method(cNativeServer, "get", ns_get, -1);
  rb_define_method(cNativeServer, "set", ns_set, -1);
}
