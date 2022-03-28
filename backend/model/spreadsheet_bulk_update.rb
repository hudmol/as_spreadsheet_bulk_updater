require 'write_xlsx'

class SpreadsheetBulkUpdate

  def initialize(resource_uri, ao_uris)
    @resource_uri = resource_uri
    @resource_id = JSONModel.parse_reference(@resource_uri).fetch(:id)
    @ao_uris = ao_uris
    @ao_ids = ao_uris.map{|uri| JSONModel.parse_reference(uri).fetch(:id)}

    @max_subrecord_counts = calculate_max_subrecords
  end

  BATCH_SIZE = 200

  class StringColumn
    attr_accessor :column, :label

    def initialize(label)
      @label = label.to_s
      @column = label
    end

    def value_for(column_value)
      column_value
    end
  end

  class EnumColumn < StringColumn
    def initialize(label, enum_name)
      super("#{label}_id".intern)
      @label = label.to_s
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
  end

  SUBRECORDS_OF_INTEREST = [:date]
  FIELDS_OF_INTEREST = {
    :basic_information => [
      StringColumn.new(:title),
      EnumColumn.new(:level, 'archival_record_level'),
      BooleanColumn.new(:publish),
    ],
    :date => [
      EnumColumn.new(:date_type, 'date_type'),
      EnumColumn.new(:label, 'date_label'),
      StringColumn.new(:expression),
      StringColumn.new(:begin),
      StringColumn.new(:end),
    ],
    :extent => [
      EnumColumn.new(:portion, 'extent_portion'),
      StringColumn.new(:number),
      EnumColumn.new(:extent_type, 'extent_extent_type'),
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
    headers = ['ID', 'Version']

    FIELDS_OF_INTEREST.fetch(:basic_information).each do |field|
      headers << I18n.t("archival_object.#{field.label}", :default => field.label)
    end

    subrecords_iterator do |subrecord, index|
      subrecord_label = I18n.t("#{subrecord}._singular")
      FIELDS_OF_INTEREST.fetch(subrecord).each do |field|
        field_label = I18n.t("#{subrecord}.#{field.label}", :default => field.label)
        headers << "#{subrecord_label} #{index + 1} #{field_label}"
      end
    end

    headers
  end

  def machine_readable_headers
    headers = ['id', 'lock_version']

    FIELDS_OF_INTEREST.fetch(:basic_information).each do |field|
      headers << field.label
    end

    subrecords_iterator do |subrecord, index|
      FIELDS_OF_INTEREST.fetch(subrecord).each do |field|
        headers << "#{subrecord}/#{index}/#{field.label}"
      end
    end

    headers
  end

  def dataset_iterator(&block)
    DB.open do |db|
      @ao_ids.each_slice(BATCH_SIZE) do |batch|
        base_fields = [:id, :lock_version] + FIELDS_OF_INTEREST.fetch(:basic_information).map{|field| field.column}
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
            subrecord_datasets[subrecord][row[:archival_object_id]] << FIELDS_OF_INTEREST.fetch(subrecord).map{|field| [field.label, field.value_for(row[field.column])]}.to_h
          end
        end

        pp subrecord_datasets

        base.each do |row|
          current_row = [
            row[:id],
            row[:lock_version],
          ]

          FIELDS_OF_INTEREST.fetch(:basic_information).each do |field|
            current_row << field.value_for(row[field.column])
          end

          subrecords_iterator do |subrecord, index|
            subrecord_data = subrecord_datasets.fetch(subrecord).fetch(row[:id], []).fetch(index, {})
            FIELDS_OF_INTEREST.fetch(subrecord).each do |field|
              current_row << subrecord_data.fetch(field.label, nil)
            end
          end

          block.call(current_row)
        end
      end
    end
  end

  def to_stream
    pp SUBRECORDS_OF_INTEREST
    pp FIELDS_OF_INTEREST

    io = StringIO.new
    wb = WriteXLSX.new(io)

    # give us a 'locked' formatter
    locked = wb.add_format
    locked.set_locked(1)

    sheet = wb.add_worksheet('Updates')

    # protect the sheet to ensure `locked` formatting work
    sheet.protect

    sheet.write_row(0, 0, human_readable_headers)
    sheet.write_row(1, 0, machine_readable_headers)

    rowidx = 2
    dataset_iterator do |row_values|
      sheet.write_row(rowidx, 0, row_values)
      rowidx += 1
    end

    # lock the readonly bits
    sheet.set_row(0, nil, locked)
    sheet.set_row(1, nil, locked)
    sheet.set_column(0, nil, locked)
    sheet.set_column(1, nil, locked)

    wb.close
    io.string
  end
end