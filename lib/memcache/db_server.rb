class Memcache
  class DBServer
    attr_reader :db, :table

    def initialize(opts)
      @table = opts[:table]
      @db    = opts[:db] || ActiveRecord::Base.connection.raw_connection
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
      db.query %{
        SELECT value FROM #{table}
         WHERE key = #{quote(key)} AND #{expiry_clause}
      }
    end

    def get_multi(keys)
      keys = keys.collect {|key| quote(key)}.join(',')
      db.query %{
        SELECT value FROM #{table}
         WHERE key IN (#{keys}) AND #{expiry_clause}
      }
    end

    def incr(key, amount = 1)
      db.transaction do
        value = get(key)
        return unless value        
        return unless value =~ /^\d+$/
        
        value = value.to_i + amount
        db.query %{
          UPDATE #{table} SET value = #{quote(value)}, updated_at = NOW()
           WHERE key = #{quote(key)}
        }
      end
      value
    end

    def delete(key, expiry = 0)
      if expiry
        db.query %{
          UPDATE #{table} SET expires_at = NOW() + interval '#{expiry} seconds'
           WHERE key = #{quote(key)} AND #{expiry_clause(expiry)}
        }
      else
        db.query %{
          DELETE FROM #{table}
           WHERE key = #{quote(key)}
        }
      end
    end

    def set(key, value, expiry = 0)
      store(:set, key, value, expiry)
    end

    def add(key, value, expiry = 0)
      store(:add, key, value, expiry)
    end

  private
 
    def store(method, key, value, expiry)
      begin
        db.exec %{
          INSERT INTO #{table} (key, value, updated_at, expires_at)
            VALUES (#{quote(key)}, #{quote(value)}, NOW(), #{expiry_sql(expiry)})
        }
      rescue ActiveRecord::StatementInvalid => e
        return nil if method == :add 
        db.exec %{
          UPDATE #{table}
           SET value = #{quote(value)}, updated_at = NOW(), expires_at = #{expiry_sql(expiry)}
           WHERE key = #{quote(key)}
        }
      end
      value
    end
        
    def quote(string)
      string.to_s.gsub(/'/,"\'")
      "'#{string}'"
    end

    def expiry_clause(expiry = 0)
      "expires_at IS NULL OR expires_at > '#{expiry == 0 ? 'NOW()' : expiry_sql(expiry)}'"
    end

    def expiry_sql(expiry)
      expiry == 0 ? 'NULL' : "NOW() + interval '#{expiry} seconds'"
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
