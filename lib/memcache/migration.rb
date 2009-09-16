class Memcache
  class Migration < ActiveRecord::Migration
    class << self
      attr_accessor :table
    end
    
    def self.up
      create_table table, :id => false do |t|
        t.string    :key
        t.text      :value
        t.timestamp :expires_at
        t.timestamp :updated_at
      end
      
      add_index table, [:key], :unique => true
      add_index table, [:expires_at]
    end
    
    def self.down
      drop_table table
    end
  end
end
