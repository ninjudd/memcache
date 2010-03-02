require 'active_record'
require 'memcache/migration'

class PGconn
  def self.quote_ident(name)
    %("#{name}")
  end
end

class Memcache
  class PGServer < Base
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
      db.exec("TRUNCATE #{table}")
    end

    def get(keys)
      return get([keys])[keys.to_s] unless keys.kind_of?(Array)
      return {} if keys.empty?

      keys = keys.collect {|key| quote(key.to_s)}.join(',')
      sql = %{
        SELECT key, value FROM #{table}
          WHERE key IN (#{keys}) AND #{prefix_clause} AND #{expiry_clause}
      }
      results = {}
      db.query(sql).each do |key, value|
        results[key] = value
      end
      results
    end

    def incr(key, amount = 1)
      transaction do
        value = get(key)
        return unless value
        return unless value =~ /^\d+$/

        value = value.to_i + amount
        value = 0 if value < 0
        db.exec %{
          UPDATE #{table} SET value = #{quote(value)}, updated_at = NOW()
            WHERE key = #{quote(key)} AND #{prefix_clause}
        }
        value
      end
    end

    def decr(key, amount = 1)
      incr(key, -amount)
    end

    def delete(key)
      result = db.exec %{
        DELETE FROM #{table}
          WHERE key = #{quote(key)} AND #{prefix_clause}
      }
      result.cmdtuples == 1
    end

    def set(key, value, expiry = 0)
      transaction do
        delete(key)
        insert(key, value, expiry)
      end
      value
    end

    def add(key, value, expiry = 0)
      delete_expired(key)
      insert(key, value, expiry)
      value
    rescue PGError => e
      nil
    end

    def replace(key, value, expiry = 0)
      delete_expired(key)
      result = update(key, value, expiry)
      result.cmdtuples == 1 ? value : nil
    end

    def append(key, value)
      delete_expired(key)
      result = db.exec %{
        UPDATE #{table}
          SET value = value || #{quote(value)}, updated_at = NOW()
          WHERE key = #{quote(key)} AND #{prefix_clause}
      }
      result.cmdtuples == 1
    end

    def prepend(key, value)
      delete_expired(key)
      result = db.exec %{
        UPDATE #{table}
          SET value = #{quote(value)} || value, updated_at = NOW()
          WHERE key = #{quote(key)} AND #{prefix_clause}
      }
      result.cmdtuples == 1
    end

  private

    def insert(key, value, expiry = 0)
      db.exec %{
        INSERT INTO #{table} (prefix, key, value, updated_at, expires_at)
          VALUES (#{quoted_prefix}, #{quote(key)}, #{quote(value)}, NOW(), #{expiry_sql(expiry)})
      }
    end

    def update(key, value, expiry = 0)
      db.exec %{
        UPDATE #{table}
          SET value = #{quote(value)}, updated_at = NOW(), expires_at = #{expiry_sql(expiry)}
          WHERE key = #{quote(key)} AND #{prefix_clause}
      }
    end

    def transaction
      return yield if @in_transaction

      begin
        @in_transaction = true
        db.exec('BEGIN')
        value = yield
        db.exec('COMMIT')
        value
      rescue Exception => e
        db.exec('ROLLBACK')
        raise e
      ensure
        @in_transaction = false
      end
    end

    def quote(string)
      string.to_s.gsub(/'/,"\'")
      "'#{string}'"
    end

    def delete_expired(key)
      db.exec "DELETE FROM #{table} WHERE key = #{quote(key)} AND #{prefix_clause} AND NOT (#{expiry_clause})"
    end

    def expiry_clause
      "expires_at IS NULL OR expires_at > NOW()"
    end

    def expiry_sql(expiry)
      expiry = Time.at(expiry) if expiry > 60*60*24*30
      if expiry.kind_of?(Time)
        quote(expiry.to_s(:db))
      else
        expiry == 0 ? 'NULL' : "NOW() + interval '#{expiry} seconds'"
      end
    end

    def quoted_prefix
      quote(prefix || '')
    end

    def prefix_clause
      "prefix = #{quoted_prefix}"
    end
  end
end
