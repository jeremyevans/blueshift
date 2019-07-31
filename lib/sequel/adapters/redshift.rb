require 'sequel/adapters/postgres'

module Sequel
  module Redshift
    include Postgres

    class Database < Postgres::Database
      set_adapter_scheme :redshift
      SORTSTYLES = [:compound, :interleaved].freeze
      DISTSTYLES = [:even, :key, :all].freeze

      def redshift_version
        return @redshift_version if defined?(@redshift_version)
        DB.get(Sequel.function(:version)) =~ /Redshift ([\d.]+)/
        @redshift_version = if $1
          $1.gsub('.', '').to_i
        end
      end

      def values(rows)
        row1, *rows, row2 = rows 
        ds = select(*row1.map.with_index{|v, i| Sequel.as(v, :"column#{i+1}")})
        rows.each do |row|
          ds = ds.union(select(*row), :all=>true, :from_self=>false)
        end
        if row2
          ds = ds.union(select(*row2), :all=>true)
        end
        ds
      end

      def serial_primary_key_options
        # redshift doesn't support serial/identity type
        {:primary_key => true, :type=>Integer}
      end

      def connection_configuration_sqls(_=nil)
        []
      end

      def supports_foreign_key_parsing?
        false
      end

      def supports_index_parsing?
        false
      end

      private

      def dataset_class_default
        Dataset
      end

      def type_literal_generic_string(column)
        super(column.merge(text: false))
      end

      def create_table_sql(name, generator, options)
        validate_options!(options)
        sql = super
        sql += diststyle_sql(options)
        sql += distkey_sql(options)
        sql += sortstyle_sql(options)

        sql
      end

      def diststyle_sql(options)
        if options[:diststyle]
          " DISTSTYLE #{options[:diststyle].to_s.upcase}"
        end.to_s
      end

      def distkey_sql(options)
        if options[:distkey]
          " DISTKEY (#{options[:distkey]})"
        end.to_s
      end

      def sortstyle_sql(options)
        if options[:sortkeys]
          style = options[:sortstyle].to_s.upcase
          " #{style} SORTKEY (#{Array(options[:sortkeys]).join(', ')})".squeeze(' ')
        end.to_s
      end

      def validate_options!(options)
        raise ArgumentError, 'sortstyle must be one of :compound or :interleaved' if invalid?(options[:sortstyle], SORTSTYLES)
        raise ArgumentError, 'diststyle must be one of :even, key, or :all' if invalid?(options[:diststyle], DISTSTYLES)
      end

      def invalid?(value, allowed)
        value && !allowed.include?(value)
      end

      # OVERRIDE for Redshift. Now always expect the "id" column to be the primary key
      # The dataset used for parsing table schemas, using the pg_* system catalogs.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        ds = metadata_dataset.select(:pg_attribute__attname___name,
            SQL::Cast.new(:pg_attribute__atttypid, :integer).as(:oid),
            SQL::Cast.new(:basetype__oid, :integer).as(:base_oid),
            SQL::Function.new(:format_type, :basetype__oid, :pg_type__typtypmod).as(:db_base_type),
            SQL::Function.new(:format_type, :pg_type__oid, :pg_attribute__atttypmod).as(:db_type),
            SQL::Function.new(:pg_get_expr, :pg_attrdef__adbin, :pg_class__oid).as(:default),
            SQL::BooleanExpression.new(:NOT, :pg_attribute__attnotnull).as(:allow_null),
            SQL::Function.new(:COALESCE, SQL::BooleanExpression.from_value_pairs(:pg_attribute__attnum => SQL::Function.new(:ANY,
              SQL::Function.new(:string_to_array, SQL::Function.new(:textin, SQL::Function.new(:int2vectorout, :pg_index__indkey)), ' ')
            )), false).as(:primary_key)).
          from(:pg_class).
          join(:pg_attribute, :attrelid=>:oid).
          join(:pg_type, :oid=>:atttypid).
          left_outer_join(:pg_type___basetype, :oid=>:typbasetype).
          left_outer_join(:pg_attrdef, :adrelid=>:pg_class__oid, :adnum=>:pg_attribute__attnum).
          left_outer_join(:pg_index, :indrelid=>:pg_class__oid, :indisprimary=>true).
          filter(:pg_attribute__attisdropped=>false).
          filter{|o| o.pg_attribute__attnum > 0}.
          filter(:pg_class__oid=>regclass_oid(table_name, opts)).
          order(:pg_attribute__attnum)
        ds.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          if row[:base_oid]
            row[:domain_oid] = row[:oid]
            row[:oid] = row.delete(:base_oid)
            row[:db_domain_type] = row[:db_type]
            row[:db_type] = row.delete(:db_base_type)
          else
            row.delete(:base_oid)
            row.delete(:db_base_type)
          end
          row[:type] = schema_column_type(row[:db_type])
          if row[:primary_key]
            row[:auto_increment] = !!(row[:default] =~ /\Anextval/io)
          end
          [m.call(row.delete(:name)), row]
        end
      end
    end

    class Dataset < Postgres::Dataset
      DURATION_UNITS = [:years, :months, :days, :hours, :minutes, :seconds].freeze
      DEF_DURATION_UNITS = DURATION_UNITS.zip(DURATION_UNITS.map{|s| s.to_s.freeze}).freeze
      # Try to make date arithmetic mostly work by converting years and
      # months to days using average days per year and average days per
      # month.
      def date_add_sql_append(sql, da)
        h = da.interval
        expr = da.expr
        cast_type = da.cast_type || Time
        interval = String.new

        each_valid_interval_unit(h, DEF_DURATION_UNITS) do |value, sql_unit|
          case sql_unit 
          when /years/
            value = (value * 365.24).round
            sql_unit = 'days'
          when /months/
            value = (value * 30.44).round
            sql_unit = 'days'
          end

          interval << "#{value} #{sql_unit} "
        end

        if interval.empty?
          return literal_append(sql, Sequel.cast(expr, cast_type))
        else
          return complex_expression_sql_append(sql, :+, [Sequel.cast(expr, cast_type), Sequel.cast(interval, :interval)])
        end
      end

      # Redshift does not support WITH in INSERT, DELETE, or UPDATE.
      def supports_cte?(type=:select)
        type == :select
      end

      # Redshift supports WITH in some subqueries.
      def supports_cte_in_subqueries?
        supports_cte?
      end

      # Redshift doesn't support RETURNING statement
      def insert_returning_sql(sql)
        # do nothing here
        sql
      end

      # PostgreSQL 8.4+ supports window functions
      def supports_window_functions?
        true
      end
    end
  end
end
