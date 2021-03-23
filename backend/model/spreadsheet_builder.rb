require 'write_xlsx'

class SpreadsheetBuilder

  def initialize(resource_uri, ao_uris)
    @resource_uri = resource_uri
    @resource_id = JSONModel.parse_reference(@resource_uri).fetch(:id)
    @ao_uris = ao_uris
    @ao_ids = ao_uris.map{|uri| JSONModel.parse_reference(uri).fetch(:id)}

    @max_subrecord_counts = calculate_max_subrecords
  end

  BATCH_SIZE = 200
  SHEET_NAME = 'Updates'

  class StringColumn
    attr_accessor :name, :column, :index, :jsonmodel, :width, :locked, :path_prefix

    def initialize(jsonmodel, name, opts = {})
      @jsonmodel = jsonmodel
      @name = name
      @header_label = opts.fetch(:header_label, nil)
      @column = opts.fetch(:column, name).intern
      @width = opts.fetch(:width, nil)
      @locked = opts.fetch(:locked, false)
      @path_prefix = opts.fetch(:path, jsonmodel)
    end

    def value_for(column_value)
      column_value
    end

    def header_label
      if @header_label.nil?
        if @index.nil?
          @header_label = I18n.t("#{jsonmodel}.#{name}", :default => name)
        else
          @header_label = "#{I18n.t("#{jsonmodel}._singular")} #{index + 1} - #{I18n.t("#{jsonmodel}.#{name}", :default => name)}"
        end
      end

      @header_label
    end

    def path
      if jsonmodel == :archival_object
        name.to_s
      else
        [@path_prefix, index, name].join('/')
      end
    end

    def sanitise_incoming_value(value)
      value
    end
  end

  class EnumColumn < StringColumn
    attr_accessor :enum_name

    def initialize(jsonmodel, name, enum_name, opts = {})
      super(jsonmodel, name, {:column => "#{name}_id"}.merge(opts))
      @enum_name = enum_name
    end

    def value_for(column_value)
      BackendEnumSource.value_for_id(@enum_name, column_value)
    end
  end

  class BooleanColumn < StringColumn
    def value_for(column_value)
      (column_value == 1).to_s
    end

    def sanitise_incoming_value(value)
      value == 'true'
    end
  end

  SUBRECORDS_OF_INTEREST = [:date, :extent]
  FIELDS_OF_INTEREST = {
    :archival_object => [
      StringColumn.new(:archival_object, :id, :header_label => "Id", :locked => true),
      StringColumn.new(:archival_object, :lock_version, :header_label => "Version", :locked => true),
      StringColumn.new(:archival_object, :title, :width => 30),
      EnumColumn.new(:archival_object, :level, 'archival_record_level', :width => 15),
      BooleanColumn.new(:archival_object, :publish),
    ],
    :date => [
      EnumColumn.new(:date, :date_type, 'date_type', :path => :dates),
      EnumColumn.new(:date, :label, 'date_label', :path => :dates),
      StringColumn.new(:date, :expression, :width => 15, :path => :dates),
      StringColumn.new(:date, :begin, :width => 10, :path => :dates),
      StringColumn.new(:date, :end, :width => 10, :path => :dates),
    ],
    :extent => [
      EnumColumn.new(:extent, :portion, 'extent_portion', :width => 15, :path => :extents),
      StringColumn.new(:extent, :number, :width => 15, :path => :extents),
      EnumColumn.new(:extent, :extent_type, 'extent_extent_type', :width => 15, :path => :extents),
    ],
  }

  def calculate_max_subrecords
    results = {}

    DB.open do |db|
      SUBRECORDS_OF_INTEREST.each do |subrecord|
        results[subrecord] = db[subrecord]
                               .filter(:archival_object_id => @ao_ids)
                               .group_and_count(:archival_object_id)
                               .max(:count) || 0
      end
    end

    results
  end

  def build_filename
    "bulk_update.resource_#{@resource_id}.#{Date.today.iso8601}.xlsx"
  end

  def subrecords_iterator
    SUBRECORDS_OF_INTEREST
      .map do |subrecord|
      @max_subrecord_counts.fetch(subrecord).times do |i|
        yield(subrecord, i)
      end
    end
  end

  def human_readable_headers
    all_columns.map{|col| col.header_label}
  end

  def machine_readable_headers
    all_columns.map{|col| col.path}
  end

  def all_columns
    return @columns if @columns

    result = []

    FIELDS_OF_INTEREST.fetch(:archival_object).each do |column|
      result << column
    end

    subrecords_iterator do |subrecord, index|
      FIELDS_OF_INTEREST.fetch(subrecord).each do |column|
        column = column.clone
        column.index = index
        result << column
      end
    end

    @columns = result
  end

  def dataset_iterator(&block)
    DB.open do |db|
      @ao_ids.each_slice(BATCH_SIZE) do |batch|
        base_fields = [:id, :lock_version] + FIELDS_OF_INTEREST.fetch(:archival_object).map{|field| field.column}
        base = ArchivalObject
                .filter(:id => batch)
                .select(*base_fields)

        subrecord_datasets = {}
        SUBRECORDS_OF_INTEREST.each do |subrecord|
          subrecord_fields = [:archival_object_id] + FIELDS_OF_INTEREST.fetch(subrecord).map{|field| field.column}

          subrecord_datasets[subrecord] = {}

          db[subrecord]
            .filter(:archival_object_id => batch)
            .select(*subrecord_fields)
            .each do |row|
            subrecord_datasets[subrecord][row[:archival_object_id]] ||= []
            subrecord_datasets[subrecord][row[:archival_object_id]] << FIELDS_OF_INTEREST.fetch(subrecord).map{|field| [field.name, field.value_for(row[field.column])]}.to_h
          end
        end

        base.each do |row|
          locked_column_indexes = []

          current_row = []

          all_columns.each_with_index do |column, index|
            locked_column_indexes <<  index if column.locked

            if column.jsonmodel == :archival_object
              current_row << ColumnAndValue.new(column.value_for(row[column.column]), column)
            else
              subrecord_data = subrecord_datasets.fetch(column.jsonmodel).fetch(row[:id], []).fetch(column.index, nil)
              if subrecord_data
                current_row << ColumnAndValue.new(subrecord_data.fetch(column.name, nil), column)
              else
                current_row << ColumnAndValue.new(nil, column)
                locked_column_indexes << current_row.length - 1
              end
            end
          end

          block.call(current_row, locked_column_indexes)
        end
      end
    end
  end

  ColumnAndValue = Struct.new(:value, :column)

  def to_stream
    io = StringIO.new
    wb = WriteXLSX.new(io)

    # give us `locked` and `unlocked` formatters
    locked = wb.add_format
    locked.set_locked(1)
    locked.set_color('gray')
    locked.set_size(8)
    unlocked = wb.add_format
    unlocked.set_locked(0)

    # and a special one for the human headers row
    human_header_format = wb.add_format
    human_header_format.set_locked(1)
    human_header_format.set_bold
    human_header_format.set_size(12)

    sheet = wb.add_worksheet(SHEET_NAME)
    sheet.freeze_panes(1,3)

    # protect the sheet to ensure `locked` formatting work
    sheet.protect

    sheet.write_row(0, 0, human_readable_headers)
    sheet.write_row(1, 0, machine_readable_headers)
    sheet.set_row(0, nil, human_header_format)
    sheet.set_row(1, nil, locked)

    rowidx = 2
    dataset_iterator do |row_values, locked_column_indexes|
      row_values.each_with_index do |columnAndValue, i|
        sheet.write(rowidx, i, columnAndValue.value, locked_column_indexes.include?(i) ? locked : unlocked)
      end

      rowidx += 1
    end

    enum_sheet = wb.add_worksheet('Enums')
    enum_sheet.protect
    enum_counts_by_col = {}
    all_columns.each_with_index do |column, col_index|
      if column.is_a?(EnumColumn)
        enum_sheet.write(0, col_index, column.enum_name)
        BackendEnumSource.values_for(column.enum_name).each_with_index do |enum, enum_index|
          enum_sheet.write(enum_index+1, col_index, enum)
        end
        enum_counts_by_col[col_index] = BackendEnumSource.values_for(column.enum_name).length
      elsif column.is_a?(BooleanColumn)
        enum_sheet.write(0, col_index, 'boolean')
        enum_sheet.write(1, col_index, 'true')
        enum_sheet.write(2, col_index, 'false')
        enum_counts_by_col[col_index] = 2
      end
    end

    all_columns.each_with_index do |column, col_idx|
      if column.is_a?(EnumColumn) || column.is_a?(BooleanColumn)
        sheet.data_validation(2, col_idx, 2 + @ao_ids.length, col_idx,
                              {
                                'validate' => 'list',
                                'source' => "=Enums!$#{col_ref_for_index(col_idx)}$2:$#{col_ref_for_index(col_idx)}$#{enum_counts_by_col.fetch(col_idx)+1}"
                              })
      end

      if column.width
        sheet.set_column(col_idx, col_idx, column.width)
      end
    end

    wb.close
    io.string
  end

  LETTERS = ('A'..'Z').to_a
  def col_ref_for_index(index)
    LETTERS[index]
  end

  def self.column_for_path(path)
    @column_cache ||= {}

    return @column_cache.fetch(path) if @column_cache && @column_cache.has_key?(path)

    if path =~ /^([a-z-_]+)\/([0-9]+)\/(.*)$/
      path_prefix = $1.intern
      index = Integer($2)
      field = $3.intern

      column = FIELDS_OF_INTEREST.values.flatten.find{|col| col.name == field && col.path_prefix == path_prefix}

      raise "Column definition not found for #{path}" if column.nil?

      column = column.clone
      column.index = index

      @column_cache[path] = column
    else
      column = FIELDS_OF_INTEREST.fetch(:archival_object).find{|col| col.name == path.intern}

      raise "Column definition not found for #{path}" if column.nil?

      @column_cache[path] = column.clone
    end

    @column_cache[path]
  end
end
