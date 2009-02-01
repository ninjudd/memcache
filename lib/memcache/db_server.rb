class Memcache
  class DBServer
    attr_reader :db, :table

    def initialize(opts)
      @table = opts[:table]
      @db    = opts[:db] || ActiveRecord::Base.connection
    end

    def name
      @name ||= begin
        db_config = db.instance_variable_get(:@config)
        "#{db_config[:host]}:#{db_config[:database]}:#{table}"
      end
    end

    def flush_all(delay = nil)
      db.execute("DELETE FROM #{table}")
    end

    def get(key)
      db.select_value %{
        SELECT value FROM #{table}
         WHERE key = '#{key}' AND #{expiry_clause(Time.now)}
      }
    end

    def get_multi(keys)
      keys = keys.collect {|key| "'#{key}'"}.join(',')
      db.select_values %{
        SELECT value FROM #{table}
         WHERE key IN (#{keys}) AND #{expiry_clause(Time.now)}
      }
    end

    def incr(key, amount = 1)
      db.transaction do
        value = get(key)
        return unless value        
        return unless value =~ /^\d+$/
        
        value = value.to_i + amount
        db.execute %{
          UPDATE #{table} SET value = #{value}, updated_at = '#{Time.now.to_s(:db)}'
           WHERE key = '#{key}'
        }
      end
      value
    end

    def delete(key, expiry = nil)
      if expiry
        expires_at = Time.now + expiry
        db.execute %{
          UPDATE #{table} SET expires_at = '#{expires_at.to_s(:db)}'
           WHERE key = #{key} AND #{expiry_clause(expires_at)}
        }

        old_expiry = @expiry[key.to_s] || expiry
        @expiry[key.to_s] = [old_expiry, expiry].min
      else
        db.execute %{
          DELETE FROM #{table}
           WHERE key = #{key}
        }
      end
    end

    def set(key, value, expiry = nil)
      store(:set, key, value, expiry)
    end

    def add(key, value, expiry = nil)
      store(:add, key, value, expiry)
    end

  private
 
    def store(method, key, value, expiry)
      

      expires_at = Time.now + expiry
      begin
        sql = %{
          INSERT INTO #{table} (key, value, updated_at, expires_at)
            VALUES ('#{key}', ?, ?, ?)
        }
        ActiveRecord::Base.send(:sanitize_sql, condition)

        db.execute 
      rescue ActiveRecord::StatementInvalid => e
        return nil if method == :add 
        db.execute %{
          UPDATE #{table}
           SET value = '#{value}', updated_at = '#{Time.now.to_s(:db)}', expires_at = '#{expires_at.to_s(:db)}'
           WHERE key = '#{key}'
        }
      end
      value
    end

    def expiry_clause(expires_at)
      "expires_at IS NULL OR expires_at > '#{expires_at.to_s(:db)}'"
    end
    
    def quote_key(key)
      "'#{key}'"
    end
  end
end

class MemcacheDBMigration < ActiveRecord::Migration
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

