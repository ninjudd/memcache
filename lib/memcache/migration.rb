class Memcache
  class Migration < ActiveRecord::Migration
    class << self
      attr_accessor :table
    end

    def self.up
      create_table table, :id => false do |t|
        t.string    :prefix, :null => false
        t.string    :key,    :null => false
        t.text      :value,  :null => false
        t.timestamp :expires_at
        t.timestamp :updated_at
      end

      add_index table, [:prefix, :key], :unique => true
      add_index table, [:expires_at]
    end

    def self.down
      drop_table table
    end
  end
end
