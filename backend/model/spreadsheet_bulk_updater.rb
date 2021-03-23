class SpreadsheetBulkUpdater

  extend JSONModel

  BATCH_SIZE = 128

  def self.run(filename, job)
    check_sheet(filename)
    errors = []

    updated_count = 0
    ao_ids = extract_ao_ids(filename)

    DB.open(true) do
      ao_ids.each_slice(BATCH_SIZE) do |batch|
        to_process = {}
        each_row(filename) do |row, idx|
          next if row.empty?

          ao_id = Integer(row.fetch('id'))

          next unless batch.include?(ao_id)

          to_process[ao_id] = row
        end

        ao_objs = batch.map {|id| ArchivalObject[id]}
        ao_jsons = ArchivalObject.sequel_to_jsonmodel(ao_objs)

        ao_objs.zip(ao_jsons).each do |ao, ao_json|
          record_changed = false

          row = to_process.fetch(ao.id)

          begin
            row.values.each do |path, value|
              column = SpreadsheetBuilder.column_for_path(path)

              if column.jsonmodel == :archival_object
                next if column.name == :id

                # Validate the lock_version
                if column.name == :lock_version
                  if Integer(value) != ao_json['lock_version']
                    errors << {
                      sheet: SpreadsheetBuilder::SHEET_NAME,
                      json_property: path,
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
              else
                if (subrecord_to_update = Array(ao_json[column.path_prefix]).fetch(column.index, nil))
                  clean_value = column.sanitise_incoming_value(value)
                  if subrecord_to_update[column.name.to_s] != clean_value
                    record_changed = true
                    subrecord_to_update[column.name.to_s] = clean_value
                  end
                end
              end
            end

            if record_changed
              ao_json['position'] = nil
              ao.update_from_json(ao_json)
              job.write_output("Updated archival object #{ao.id} - #{ao_json.display_string}")
              updated_count += 1
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
      updated: updated_count,
    }
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
    pp "TODO something clever"
  end

  def self.each_row(filename, sheet_specifier = SpreadsheetBuilder::SHEET_NAME)
    headers = nil

    XLSXStreamingReader.new(filename).each(sheet_specifier).each_with_index do |row, idx|
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
    def has_heading?(column)
      self.values.include?(column.heading)
    end

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

end