require 'write_xlsx'

class SpreadsheetBuilder

  def initialize(resource_uri, ao_uris)
    @resource_uri = resource_uri
    @resource_id = JSONModel.parse_reference(@resource_uri).fetch(:id)
    @ao_uris = []
    @ao_ids = []

    ao_uris.each do |uri|
      parsed = JSONModel.parse_reference(uri)
      if parsed[:type] == 'archival_object'
        @ao_uris << uri
        @ao_ids << parsed.fetch(:id)
      end
    end

    @max_subrecord_counts = calculate_max_subrecords
  end

  BATCH_SIZE = 200
  SHEET_NAME = 'Updates'

  class StringColumn
    attr_accessor :name, :column, :index, :jsonmodel, :width, :locked, :property_name

    def initialize(jsonmodel, name, opts = {})
      @jsonmodel = jsonmodel
      @name = name
      @header_label = opts.fetch(:header_label, nil)
      @column = opts.fetch(:column, name).intern
      @width = opts.fetch(:width, nil)
      @locked = opts.fetch(:locked, false)
      @property_name = opts.fetch(:property_name, jsonmodel).to_s
      @i18n = opts.fetch(:i18n, I18n.t("#{@jsonmodel}.#{@name}", :default => @name))
      @i18n_proc = opts.fetch(:i18n_proc, nil)
      @path_proc = opts.fetch(:path_proc, nil)
    end

    def value_for(column_value)
      column_value
    end

    def header_label
      if @header_label.nil?
        if @i18n_proc
          @header_label = @i18n_proc.call(self)
        else
          if @index.nil?
            @header_label = @i18n
          else
            @header_label = "#{I18n.t("#{jsonmodel}._singular")} #{index + 1} - #{@i18n}"
          end
        end
      end

      @header_label
    end

    def path
      if @path_proc
        return @path_proc.call(self)
      end

      if jsonmodel == :archival_object
        name.to_s
      else
        [@property_name, index, name].join('/')
      end
    end

    def sanitise_incoming_value(value)
      return nil if value.nil?

      value.to_s.strip
    end
  end

  class DateStringColumn < StringColumn
    def initialize(jsonmodel, name, opts = {})
      super(jsonmodel, name, opts)
    end

    def sanitise_incoming_value(value)
      return nil if value.nil?

      if value.is_a?(Date)
        value.iso8601
      elsif value.is_a?(Time)
        value.to_date.iso8601
      else
        value.to_s.strip
      end
    end
  end

  class NoteContentColumn < StringColumn
    def header_label
      "#{I18n.t('note._singular')} #{I18n.t("enumerations.note_multipart_type.#{@name}")} - #{index + 1} - Content"
    end

    def path
      [@jsonmodel.to_s, @name.to_s, @index.to_s, 'content'].join('/')
    end
  end

  class EnumColumn < StringColumn
    attr_accessor :enum_name, :skip_values

    def initialize(jsonmodel, name, enum_name, opts = {})
      super(jsonmodel, name, {:column => "#{name}_id"}.merge(opts))
      @enum_name = enum_name
      @skip_values = opts.fetch(:skip_enum_values, [])
    end

    def value_for(enum_id)
      EnumMapper.enum_id_to_spreadsheet_value(enum_id, @enum_name)
    end

    def sanitise_incoming_value(value)
      EnumMapper.spreadsheet_value_to_enum(value)
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
    ],
    :date => [
      StringColumn.new(:date, :expression, :width => 15, :property_name => :dates),
      DateStringColumn.new(:date, :begin, :width => 10, :property_name => :dates),
      DateStringColumn.new(:date, :end, :width => 10, :property_name => :dates),
      EnumColumn.new(:date, :certainty, 'date_certainty', :property_name => :dates),
    ],
    :extent => [
      EnumColumn.new(:extent, :portion, 'extent_portion', :width => 15, :property_name => :extents),
      StringColumn.new(:extent, :number, :width => 15, :property_name => :extents),
      EnumColumn.new(:extent, :extent_type, 'extent_extent_type', :width => 15, :property_name => :extents),
      StringColumn.new(:extent, :container_summary, :width => 20, :property_name => :extents),
    ],
    :instance => [
      EnumColumn.new(:instance, :instance_type, 'instance_instance_type', :property_name => :instances, :skip_enum_values => ['digital_object']),
      EnumColumn.new(:instance, :top_container_type, 'container_type', :property_name => :instances, :i18n => "Top Container Type"),    # Sorry, these are hardcoded as
      StringColumn.new(:instance, :top_container_indicator, :property_name => :instances, :i18n => "Top Container Indicator"),          # all top and sub container I18n
      StringColumn.new(:instance, :top_container_barcode, :property_name => :instances, :i18n => "Top Container Barcode"),              # are available only in the
      EnumColumn.new(:instance, :sub_container_type_2, 'container_type', :property_name => :instances, :i18n => "Child Type"),          # frontend... WHY?!
      StringColumn.new(:instance, :sub_container_indicator_2, :property_name => :instances, :i18n => "Child Indicator"),                #
      StringColumn.new(:instance, :sub_container_barcode_2, :property_name => :instances, :i18n => "Child Container Barcode"),          #
      EnumColumn.new(:instance, :sub_container_type_3, 'container_type', :property_name => :instances, :i18n => "Grandchild Type"),     # Boo.
      StringColumn.new(:instance, :sub_container_indicator_3, :property_name => :instances, :i18n => "Grandchild Indicator"),           #
    ],
  }
  # Conditions of Access, Scope and Contents, Bio/Hist note
  MULTIPART_NOTES_OF_INTEREST = [:accessrestrict, :scopecontent, :bioghist]

  EXTRA_NOTE_FIELDS = {
    :accessrestrict => [
      DateStringColumn.new(:accessrestrict, :begin, :width => 10,
                           :property_name => :rights_restriction,
                           :i18n_proc => proc{|col|
                             "#{I18n.t('note._singular')} #{I18n.t("enumerations.note_multipart_type.accessrestrict")} - #{col.index + 1} - Begin"
                           },
                           :path_proc => proc{|col|
                             ['note', col.jsonmodel.to_s, col.index.to_s, col.name.to_s].join('/')
                           }),
      DateStringColumn.new(:accessrestrict, :end, :width => 10,
                           :property_name => :rights_restriction,
                           :i18n_proc => proc{|col|
                             "#{I18n.t('note._singular')} #{I18n.t("enumerations.note_multipart_type.accessrestrict")} - #{col.index + 1} - End"
                           },
                           :path_proc => proc{|col|
                             ['note', col.jsonmodel.to_s, col.index.to_s, col.name.to_s].join('/')
                           }),
      EnumColumn.new(:accessrestrict, :local_access_restriction_type, 'restriction_type',
                     :width => 15,
                     :property_name => :rights_restriction,
                     :i18n_proc => proc{|col|
                       "#{I18n.t('note._singular')} #{I18n.t("enumerations.note_multipart_type.accessrestrict")} - #{col.index + 1} - Type"
                     },
                     :path_proc => proc{|col|
                       ['note', col.jsonmodel.to_s, col.index.to_s, col.name.to_s].join('/')
                     }),
    ]
  }

  def calculate_max_subrecords
    results = {}

    DB.open do |db|
      SUBRECORDS_OF_INTEREST.each do |subrecord|
        max = db[subrecord]
                .filter(:archival_object_id => @ao_ids)
                .group_and_count(:archival_object_id)
                .max(:count) || 0

        # Notes, Extent: At least 3 more than the max
        results[subrecord] = max + 3
      end

      # Instances are special
      instances_max = db[:instance]
        .filter(:archival_object_id => @ao_ids)
        .filter(Sequel.~(:instance_type_id => BackendEnumSource.id_for_value('instance_instance_type', 'digital_object')))
        .group_and_count(:archival_object_id)
        .max(:count) || 0

      results[:instance] = instances_max + 3

      MULTIPART_NOTES_OF_INTEREST.each do |note_type|
        notes_max = db[:note]
                      .filter(:archival_object_id => @ao_ids)
                      .filter(Sequel.like(:notes, '%"type":"'+note_type.to_s+'"%'))
                      .group_and_count(:archival_object_id)
                      .max(:count) || 0

        # Notes: At least 2 of each type
        results[note_type] = [notes_max, 2].max
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

  def instances_iterator
    @max_subrecord_counts.fetch(:instance).times do |i|
      yield(:instance, i)
    end
  end

  def notes_iterator
    MULTIPART_NOTES_OF_INTEREST
      .map do |note_type|
      @max_subrecord_counts.fetch(note_type).times do |i|
        yield(note_type, i)
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

    instances_iterator do |_, index|
      FIELDS_OF_INTEREST.fetch(:instance).each do |column|
        column = column.clone
        column.index = index
        result << column
      end
    end

    notes_iterator do |note_type, index|
      column = NoteContentColumn.new(:note, note_type, :width => 30)
      column.index = index
      result << column

      EXTRA_NOTE_FIELDS.fetch(note_type, []).each do |extra_column|
        column = extra_column.clone
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

        # Instances are special
        db[:instance]
          .join(:sub_container, Sequel.qualify(:sub_container, :instance_id) => Sequel.qualify(:instance, :id))
          .join(:top_container_link_rlshp, Sequel.qualify(:top_container_link_rlshp, :sub_container_id) => Sequel.qualify(:sub_container, :id))
          .join(:top_container, Sequel.qualify(:top_container, :id) => Sequel.qualify(:top_container_link_rlshp, :top_container_id))
          .filter(Sequel.qualify(:instance, :archival_object_id) => @ao_ids)
          .filter(Sequel.~(Sequel.qualify(:instance, :instance_type_id) => BackendEnumSource.id_for_value('instance_instance_type', 'digital_object')))
          .select(
            Sequel.as(Sequel.qualify(:instance, :archival_object_id), :archival_object_id),
            Sequel.as(Sequel.qualify(:instance, :instance_type_id), :instance_type_id),
            Sequel.as(Sequel.qualify(:top_container, :type_id), :top_container_type_id),
            Sequel.as(Sequel.qualify(:top_container, :indicator), :top_container_indicator),
            Sequel.as(Sequel.qualify(:top_container, :barcode), :top_container_barcode),
            Sequel.as(Sequel.qualify(:sub_container, :type_2_id), :sub_container_type_2_id),
            Sequel.as(Sequel.qualify(:sub_container, :indicator_2), :sub_container_indicator_2),
            Sequel.as(Sequel.qualify(:sub_container, :barcode_2), :sub_container_barcode_2),
            Sequel.as(Sequel.qualify(:sub_container, :type_3_id), :sub_container_type_3_id),
            Sequel.as(Sequel.qualify(:sub_container, :indicator_3), :sub_container_indicator_3),
          ).each do |row|
          subrecord_datasets[:instance] ||= {}
          subrecord_datasets[:instance][row[:archival_object_id]] ||= []
          subrecord_datasets[:instance][row[:archival_object_id]] << {
            :instance_type => EnumMapper.enum_id_to_spreadsheet_value(row[:instance_type_id], 'instance_instance_type'),
            :top_container_type => EnumMapper.enum_id_to_spreadsheet_value(row[:top_container_type_id], 'container_type'),
            :top_container_indicator => row[:top_container_indicator],
            :top_container_barcode => row[:top_container_barcode],
            :sub_container_type_2 => EnumMapper.enum_id_to_spreadsheet_value(row[:sub_container_type_2_id], 'container_type'),
            :sub_container_indicator_2 => row[:sub_container_indicator_2],
            :sub_container_barcode_2 => row[:sub_container_barcode_2],
            :sub_container_type_3 => EnumMapper.enum_id_to_spreadsheet_value(row[:sub_container_type_3_id], 'container_type'),
            :sub_container_indicator_3 => row[:sub_container_indicator_3],
          }
        end

        # Notes
        MULTIPART_NOTES_OF_INTEREST.each do |note_type|
          db[:note]
            .filter(:archival_object_id => batch)
            .filter(Sequel.like(:notes, '%"type":"'+note_type.to_s+'"%'))
            .select(:archival_object_id, :notes)
            .order(:archival_object_id, :id)
            .each do |row|
            note_json = ASUtils.json_parse(row[:notes])
            subrecord_datasets[note_type] ||= {}
            subrecord_datasets[note_type][row[:archival_object_id]] ||= []

            # take the first note_text for each note
            text_subnote = Array(note_json['subnotes']).detect{|subnote| subnote['jsonmodel_type'] == 'note_text'}

            note_data = {
              :content => text_subnote ? text_subnote['content'] : nil,
            }

            EXTRA_NOTE_FIELDS.fetch(note_type, []).each do |extra_column|
              value = Array(note_json.fetch(extra_column.property_name.to_s, {}).fetch(extra_column.name.to_s, nil)).first

              if extra_column.is_a?(EnumColumn)
                note_data[extra_column.name] = EnumMapper.enum_to_spreadsheet_value(value, extra_column.enum_name)
              else
                note_data[extra_column.name] = extra_column.value_for(value)
              end

            end

            subrecord_datasets[note_type][row[:archival_object_id]] << note_data
          end
        end

        base.each do |row|
          locked_column_indexes = []

          current_row = []

          all_columns.each_with_index do |column, index|
            locked_column_indexes <<  index if column.locked

            if column.jsonmodel == :archival_object
              current_row << ColumnAndValue.new(column.value_for(row[column.column]), column)
            elsif column.is_a?(NoteContentColumn)
              note_content = subrecord_datasets.fetch(column.name, {}).fetch(row[:id], []).fetch(column.index, {}).fetch(:content, nil)
              if note_content
                current_row << ColumnAndValue.new(note_content, column)
              else
                current_row << ColumnAndValue.new(nil, column)
              end
            elsif EXTRA_NOTE_FIELDS.has_key?(column.jsonmodel)
              note_field_value = subrecord_datasets.fetch(column.jsonmodel, {}).fetch(row[:id], []).fetch(column.index, {}).fetch(column.name, nil)
              if note_field_value
                current_row << ColumnAndValue.new(note_field_value, column)
              else
                current_row << ColumnAndValue.new(nil, column)
              end
            else
              subrecord_data = subrecord_datasets.fetch(column.jsonmodel, {}).fetch(row[:id], []).fetch(column.index, nil)
              if subrecord_data
                current_row << ColumnAndValue.new(subrecord_data.fetch(column.name, nil), column)
              else
                current_row << ColumnAndValue.new(nil, column)
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
    # and allow a few other basic formatting things
    sheet.protect(nil, {
        :format_columns => true,
        :format_rows => true,
        :sort => true,
      }
    )

    sheet.write_row(0, 0, human_readable_headers)
    sheet.write_row(1, 0, machine_readable_headers)
    sheet.set_row(0, nil, human_header_format)
    sheet.set_row(1, nil, locked)

    rowidx = 2
    dataset_iterator do |row_values, locked_column_indexes|
      row_values.each_with_index do |columnAndValue, i|
        if columnAndValue.value
          sheet.write_string(rowidx, i, columnAndValue.value, locked_column_indexes.include?(i) ? locked : unlocked)
        else
          sheet.write(rowidx, i, columnAndValue.value, locked_column_indexes.include?(i) ? locked : unlocked)
        end
      end

      rowidx += 1
    end

    enum_sheet = wb.add_worksheet('Enums')
    enum_sheet.protect
    enum_counts_by_col = {}
    all_columns.each_with_index do |column, col_index|
      if column.is_a?(EnumColumn)
        enum_sheet.write(0, col_index, column.enum_name)
        enum_values = BackendEnumSource.values_for(column.enum_name)
        enum_values.reject!{|value| column.skip_values.include?(value)}
        enum_values
          .map{|value| EnumMapper.enum_to_spreadsheet_value(value, column.enum_name)}
          .sort_by {|value| value.downcase}
          .each_with_index do |enum, enum_index|
          enum_sheet.write_string(enum_index+1, col_index, enum)
        end
        enum_counts_by_col[col_index] = enum_values.length
      elsif column.is_a?(BooleanColumn)
        enum_sheet.write_string(0, col_index, 'boolean')
        enum_sheet.write_string(1, col_index, 'true')
        enum_sheet.write_string(2, col_index, 'false')
        enum_counts_by_col[col_index] = 2
      end
    end

    all_columns.each_with_index do |column, col_idx|
      if column.is_a?(EnumColumn) || column.is_a?(BooleanColumn)
        sheet.data_validation(2, col_idx, 2 + @ao_ids.length, col_idx,
                              {
                                'validate' => 'list',
                                'source' => "=Enums!$#{index_to_col_reference(col_idx)}$2:$#{index_to_col_reference(col_idx)}$#{enum_counts_by_col.fetch(col_idx)+1}"
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

  # Note: zero-bosed index!
  def index_to_col_reference(n)
    if n < 26
      LETTERS.fetch(n)
    else
      index_to_col_reference((n / 26) - 1) + index_to_col_reference(n % 26)
    end
  end

  def self.column_for_path(path)
    if path =~ /^note\/(.*)\/([0-9]+)\/(.*)$/
      note_type = $1.intern
      index = Integer($2)
      field = $3.intern

      raise "Column definition not found for #{path}" unless MULTIPART_NOTES_OF_INTEREST.include?(note_type)

      column = if field == :content
                 NoteContentColumn.new(:note, note_type)
               else
                 EXTRA_NOTE_FIELDS.fetch(note_type, {}).detect{|col| col.name.intern == field}
               end

      raise "Column definition not found for #{path}" unless column

      column = column.clone
      column.index = index
      column
    elsif path =~ /^([a-z-_]+)\/([0-9]+)\/(.*)$/
      property_name = $1.intern
      index = Integer($2)
      field = $3.intern

      column = FIELDS_OF_INTEREST.values.flatten.find{|col| col.name.intern == field && col.property_name.intern == property_name}

      raise "Column definition not found for #{path}" if column.nil?

      column = column.clone
      column.index = index

      column
    else
      column = FIELDS_OF_INTEREST.fetch(:archival_object).find{|col| col.name == path.intern}

      raise "Column definition not found for #{path}" if column.nil?

      column.clone
    end
  end

  class EnumMapper
    def self.enum_id_to_spreadsheet_value(enum_id, enum_name)
      return enum_id if enum_id.to_s.empty?

      enum_value = BackendEnumSource.value_for_id(enum_name, enum_id)

      EnumMapper.enum_to_spreadsheet_value(enum_value, enum_name)
    end

    def self.enum_to_spreadsheet_value(enum_value, enum_name)
      return enum_value if enum_value.to_s.empty?

      enum_label = I18n.t("enumerations.#{enum_name}.#{enum_value}", :default => enum_value)

      "#{enum_label} [#{enum_value}]"
    end

    def self.spreadsheet_value_to_enum(spreadsheet_value)
      return spreadsheet_value if spreadsheet_value.to_s.empty?

      if spreadsheet_value.to_s =~ /\[(.*)\]$/
        $1
      elsif
        raise "Could not parse enumeration value from: #{spreadsheet_value}"
      end
    end
  end
end
