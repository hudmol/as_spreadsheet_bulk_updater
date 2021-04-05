class SpreadsheetBulkUpdater

  extend JSONModel

  BATCH_SIZE = 128

  SUBRECORD_DEFAULTS = {
    'dates' => {
      'label' => 'creation',
    },
  }

  def self.run(filename, job)
    check_sheet(filename)
    errors = []

    updated_uris = []

    column_by_path = extract_columns(filename)

    DB.open(true) do
      batch_rows(filename) do |batch|
        to_process = batch.map{|row| [Integer(row.fetch('id')), row]}.to_h

        ao_objs = ArchivalObject.filter(:id => to_process.keys).all
        ao_jsons = ArchivalObject.sequel_to_jsonmodel(ao_objs)

        ao_objs.zip(ao_jsons).each do |ao, ao_json|
          record_changed = false
          row = to_process.fetch(ao.id)
          last_column = nil

          subrecord_updates_by_index = {}

          all_text_subnotes_by_type = {}

          begin
            row.values.each do |path, value|
              column = column_by_path.fetch(path)

              last_column = column

              # fields on the AO
              if column.jsonmodel == :archival_object
                next if column.name == :id

                # Validate the lock_version
                if column.name == :lock_version
                  if Integer(value) != ao_json['lock_version']
                    errors << {
                      sheet: SpreadsheetBuilder::SHEET_NAME,
                      column: column.path,
                      row: row.row_number,
                      errors: ["Versions are out sync: #{value} record is now: #{ao_json['lock_version']}"]
                    }
                  end
                else
                  clean_value = column.sanitise_incoming_value(value)

                  if ao_json[path] != clean_value
                    record_changed = true
                    ao_json[path] = clean_value
                  end
                end

              # notes
              elsif column.jsonmodel == :note
                unless all_text_subnotes_by_type.has_key?(column.name)
                  all_text_subnotes = ao_json.notes
                                       .select{|note| note['jsonmodel_type'] == 'note_multipart' && note['type'] == column.name.to_s}
                                       .map{|note| note['subnotes']}
                                       .flatten
                                       .select{|subnote| subnote['jsonmodel_type'] == 'note_text'}

                  all_text_subnotes_by_type[column.name] = all_text_subnotes
                end

                clean_value = column.sanitise_incoming_value(value)

                if (subnote_to_update = all_text_subnotes_by_type[column.name].fetch(column.index, nil))
                  if subnote_to_update['content'] != clean_value
                    record_changed = true

                    # Can only drop a note if apply_deletes? is true
                    if clean_value.to_s.empty? && !apply_deletes?
                      errors << {
                        sheet: SpreadsheetBuilder::SHEET_NAME,
                        column: column.path,
                        row: row.row_number,
                        errors: ["Deleting a note is disabled. Use AppConfig[:spreadsheet_bulk_updater_apply_deletes] = true to enable."],
                      }
                    else
                      subnote_to_update['content'] = clean_value
                    end
                  end
                elsif !clean_value.to_s.empty?
                  record_changed = true

                  sub_note = SUBRECORD_DEFAULTS.fetch('note_text', {}).merge({
                    'jsonmodel_type' => 'note_text',
                    'content' => clean_value
                  })

                  ao_json.notes << SUBRECORD_DEFAULTS.fetch(column.jsonmodel.to_s, {}).merge({
                    'jsonmodel_type' => 'note_multipart',
                    'type' => column.name.to_s,
                    'subnotes' => [sub_note],
                  })

                  all_text_subnotes_by_type[column.name] << sub_note
                end

              # subrecords
              elsif SpreadsheetBuilder::SUBRECORDS_OF_INTEREST.include?(column.jsonmodel)
                subrecord_updates_by_index[column.property_name] ||= {}

                clean_value = column.sanitise_incoming_value(value)

                subrecord_updates_by_index[column.property_name][column.index] ||= {}
                subrecord_updates_by_index[column.property_name][column.index][column.name.to_s] = clean_value
              end
            end

            # apply subrecords to the json
            #  - update existing
            #  - add new subrecords
            #  - those not updated are deleted
            subrecord_updates_by_index.each do |jsonmodel_property, updates_by_index|
              subrecords_to_apply = []

              updates_by_index.each do |index, subrecord_updates|
                if (existing_subrecord = Array(ao_json[jsonmodel_property.to_s])[index])
                  if subrecord_updates.all?{|_, value| value.to_s.empty? } && apply_deletes?
                    # DELETE!
                    record_changed = true
                    next
                  end

                  if subrecord_updates.any?{|property, value| existing_subrecord[property] != value}
                    record_changed = true
                  end

                  subrecords_to_apply << existing_subrecord.merge(subrecord_updates)
                else
                  if subrecord_updates.values.all?{|v| v.to_s.empty? }
                    # Nothing to do!
                    next
                  end

                  record_changed = true
                  subrecords_to_apply << SUBRECORD_DEFAULTS.fetch(jsonmodel_property.to_s, {}).merge(subrecord_updates)
                end
              end

              ao_json[jsonmodel_property.to_s] = subrecords_to_apply
            end

            # drop any multipart notes with only empty sub notes
            # - drop subnotes empty note_text
            if apply_deletes?
              ao_json.notes.each do |note|
                if note['jsonmodel_type'] == 'note_multipart'
                  note['subnotes'].reject! do |subnote|
                    if subnote['jsonmodel_type'] == 'note_text' && subnote['content'].to_s.empty?
                      record_changed = true
                      true
                    else
                      false
                    end
                  end
                end
              end
              # - drop notes with empty subnotes
              ao_json.notes.reject! do|note|
                if note['jsonmodel_type'] == 'note_multipart' && note['subnotes'].empty?
                  record_changed = true
                  true
                else
                  false
                end
              end
            end

            if record_changed
              ao_json['position'] = nil
              ao.update_from_json(ao_json)
              job.write_output("Updated archival object #{ao.id} - #{ao_json.display_string}")
              updated_uris << ao_json['uri']
            end
          rescue JSONModel::ValidationException => validation_errors
            validation_errors.errors.each do |json_property, messages|
              errors << {
                sheet: SpreadsheetBuilder::SHEET_NAME,
                json_property: json_property,
                row: row.row_number,
                errors: messages,
              }
            end
          end
        end
      end

      if errors.length > 0
        raise SpreadsheetBulkUpdateFailed.new(errors)
      end
    end

    {
      updated: updated_uris.length,
      updated_uris: updated_uris,
    }
  end

  def self.extract_columns(filename)
    path_row = nil

    XLSXStreamingReader.new(filename).each(SpreadsheetBuilder::SHEET_NAME).each_with_index do |row, idx|
      next if idx == 0
      path_row = row_values(row)
      break
    end

    raise "Missing header row containing paths in #{filename}" if path_row.nil?

    path_row.map do |path|
      column = SpreadsheetBuilder.column_for_path(path)
      raise "Missing column definition for path: #{path}" if column.nil?

      [path, column]
    end.to_h
  end

  def self.extract_ao_ids(filename)
    result = []
    each_row(filename) do |row|
      next if row.empty?
      result << Integer(row.fetch('id'))
    end
    result
  end

  def self.check_sheet(filename)
    errors = []

    ao_ids = extract_ao_ids(filename)
    existing_ao_ids = ArchivalObject
                        .filter(:id => ao_ids)
                        .select(:id)
                        .map{|row| row[:id]}

    (ao_ids - existing_ao_ids).each do |missing_id|
      errors << {
        sheet: SpreadsheetBuilder::SHEET_NAME,
        row: 'N/A',
        column: 'id',
        errors: ["Archival Object not found for id: #{missing_id}"]
      }
    end

    if errors.length > 0
      raise SpreadsheetBulkUpdateFailed.new(errors)
    end
  end

  def self.batch_rows(filename)
    to_enum(:each_row, filename).each_slice(BATCH_SIZE) do |batch|
      yield batch
    end
  end

  def self.each_row(filename)
    headers = nil

    XLSXStreamingReader.new(filename).each(SpreadsheetBuilder::SHEET_NAME).each_with_index do |row, idx|
      if idx == 0
        # header label row is ignored
        next
      elsif idx == 1
        headers = row_values(row)
      else
        yield Row.new(headers.zip(row_values(row)).to_h, idx + 1)
      end
    end
  end

  class SpreadsheetBulkUpdateFailed < StandardError
    attr_reader :errors

    def initialize(errors)
      @errors = errors
    end

    def to_json
      @errors
    end
  end

  Row = Struct.new(:values, :row_number) do
    def fetch(*args)
      self.values.fetch(*args)
    end

    def empty?
      values.all?{|_, v| v.to_s.strip.empty?}
    end
  end

  def self.row_values(row)
    row.map {|s|
      result = s.to_s.strip
      result.empty? ? nil : result
    }
  end

  def self.apply_deletes?
    AppConfig.has_key?(:spreadsheet_bulk_updater_apply_deletes) && AppConfig[:spreadsheet_bulk_updater_apply_deletes] == true
  end

end
