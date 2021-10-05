class SpreadsheetBulkUpdater

  extend JSONModel

  attr_accessor :filename,  :job, :errors, :updated_uris

  BATCH_SIZE = 128

  SUBRECORD_DEFAULTS = {
    'dates' => {
      'label' => 'creation',
    },
    'instance' => {
      'jsonmodel_type' => 'instance',
      'sub_container' => {
        'jsonmodel_type' => 'sub_container',
        'top_container' => {'ref' => nil},
      }
    }
  }

  INSTANCE_FIELD_MAPPINGS = [
    ['instance_type', 'instance_type'],
  ]

  SUB_CONTAINER_FIELD_MAPPINGS = [
    ['type_2', 'sub_container_type_2'],
    ['indicator_2', 'sub_container_indicator_2'],
    ['barcode_2', 'sub_container_barcode_2'],
    ['type_3', 'sub_container_type_3'],
    ['indicator_3', 'sub_container_indicator_3']
  ]


  def self.run(filename, job)
    new(filename, job).run!
  end

  def initialize(filename, job)
    @filename = filename
    @job = job
    @errors = []
    @updated_uris = []
  end

  def run!
    # Run a cursory look over the spreadsheet
    check_sheet(filename)

    # Away!
    column_by_path = extract_columns(filename)

    DB.open(true) do |db|
      resource_id = resource_ids_in_play(filename).fetch(0)

      # before we get too crazy, let's ensure we have all the top containers
      # available to this resource
      @top_containers_in_resource = extract_top_containers_for_resource(db, resource_id)

      if create_missing_top_containers?
        top_containers_in_sheet = extract_top_containers_from_sheet(filename, column_by_path)
        create_missing_top_containers(top_containers_in_sheet, job)
      end

      batch_rows(filename) do |batch|
        to_process = batch.map{|row| [Integer(row.fetch('id')), row]}.to_h

        ao_objs = ArchivalObject.filter(:id => to_process.keys).all
        ao_jsons = ArchivalObject.sequel_to_jsonmodel(ao_objs)

        ao_objs.zip(ao_jsons).each do |ao, ao_json|
          process_row(to_process.fetch(ao.id), ao, ao_json, column_by_path)
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

  def process_row(row, ao, ao_json, column_by_path)
    record_changed = false

    subrecord_updates_by_index = {}
    instance_updates_by_index = {}

    notes_by_type = {}

    begin
      row.values.each do |path, value|
        column = column_by_path.fetch(path)

        # fields on the AO
        if column.jsonmodel == :archival_object
          record_changed = apply_archival_object_column(row, column, path, value, ao_json) || record_changed

          # notes
        elsif column.is_a?(SpreadsheetBuilder::NoteContentColumn) || SpreadsheetBuilder::EXTRA_NOTE_FIELDS.has_key?(column.jsonmodel)
          record_changed = apply_notes_column(row, column, value, ao_json, notes_by_type) || record_changed

          # subrecords
        elsif SpreadsheetBuilder::SUBRECORDS_OF_INTEREST.include?(column.jsonmodel)
          subrecord_updates_by_index[column.property_name] ||= {}

          clean_value = column.sanitise_incoming_value(value)

          subrecord_updates_by_index[column.property_name][column.index] ||= {}
          subrecord_updates_by_index[column.property_name][column.index][column.name.to_s] = clean_value

          # instances
        elsif column.jsonmodel == :instance
          instance_updates_by_index[column.index] ||= {}

          clean_value = column.sanitise_incoming_value(value)

          instance_updates_by_index[column.index][column.name.to_s] = clean_value
        end
      end

      record_changed = apply_sub_record_updates(row, ao_json, subrecord_updates_by_index) || record_changed
      record_changed = apply_instance_updates(row, ao_json, instance_updates_by_index) ||  record_changed

      if SpreadsheetBulkUpdater.apply_deletes?
        record_changed = delete_empty_notes(ao_json) || record_changed
      end

      # Apply changes to the Archival Object!
      if record_changed
        ao_json['position'] = nil
        ao.update_from_json(ao_json)
        job.write_output("Updated archival object #{ao.id} - #{ao_json.display_string}")
        updated_uris << ao_json['uri']
      end

    rescue ArgumentError => arg_error
      if arg_error.message == 'invalid date'
        errors << {
          sheet: SpreadsheetBuilder::SHEET_NAME,
          json_property: 'N/A',
          row: row.row_number,
          errors: ['Invalid date detected'],
        }
      else
        raise arg_error
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

  def apply_archival_object_column(row, column, path, value, ao_json)
    record_changed = false

    # we don't change the id!
    return record_changed if column.name == :id

    # Validate the lock_version
    if column.name == :lock_version
      if Integer(value) != ao_json['lock_version']
        errors << {
          sheet: SpreadsheetBuilder::SHEET_NAME,
          column: column.path,
          row: row.row_number,
          errors: ["Versions are out of sync: #{value} record is now: #{ao_json['lock_version']}"]
        }
      end
    else
      clean_value = column.sanitise_incoming_value(value)

      if ao_json[path] != clean_value
        record_changed = true
        ao_json[path] = clean_value
      end
    end

    record_changed
  end

  def apply_notes_column(row, column, value, ao_json, notes_by_type)
    record_changed = false

    note_type = column.is_a?(SpreadsheetBuilder::NoteContentColumn) ? column.name : column.jsonmodel

    unless notes_by_type.has_key?(note_type)
      notes_by_type[note_type] = ao_json.notes
                                   .select{|note| note['jsonmodel_type'] == 'note_multipart' && note['type'] == note_type.to_s}
    end

    clean_value = column.sanitise_incoming_value(value)

    note_to_update = notes_by_type[note_type].fetch(column.index, nil)

    if note_to_update.nil? && !clean_value.to_s.empty?
      # we need to create a new note!
      record_changed = true

      note_to_update = SUBRECORD_DEFAULTS.fetch(column.jsonmodel.to_s, {}).merge({
                                                                                   'jsonmodel_type' => 'note_multipart',
                                                                                   'type' => note_type.to_s,
                                                                                   'subnotes' => [],
                                                                                 })

      notes_by_type[note_type][column.index] = note_to_update
      ao_json.notes << note_to_update
    end

    if note_to_update
      # Apply content
      if column.is_a?(SpreadsheetBuilder::NoteContentColumn)
        # Update the first text note
        if (first_text_note = note_to_update['subnotes'].detect{|subnote| subnote['jsonmodel_type'] == 'note_text'})
          if clean_value != first_text_note['content']
            record_changed = true

            if clean_value.to_s.empty? && !SpreadsheetBulkUpdater.apply_deletes?
              errors << {
                sheet: SpreadsheetBuilder::SHEET_NAME,
                column: column.path,
                row: row.row_number,
                errors: ["Deleting a note is disabled. Use AppConfig[:spreadsheet_bulk_updater_apply_deletes] = true to enable."],
              }
            else
              first_text_note['content'] = clean_value
            end
          end

          # Add a text note!
        elsif !clean_value.to_s.empty?
          record_changed = true
          note_to_update['subnotes'] << SUBRECORD_DEFAULTS.fetch('note_text', {}).merge({
                                                                                          'jsonmodel_type' => 'note_text',
                                                                                          'content' => clean_value
                                                                                        })
        end

      # Update the extra note field
      else
        # FIXME Assuming the column property name gives the path on the note
        note_to_update[column.property_name.to_s] ||= {}
        note_path_to_update = note_to_update[column.property_name.to_s]

        if column.name.to_s == 'local_access_restriction_type'
          # this is an array!
          clean_value = clean_value.to_s.empty? ? [] : [clean_value]
        end

        if note_path_to_update[column.name.to_s] != clean_value
          record_changed = true
          note_path_to_update[column.name.to_s] = clean_value
        end
      end
    end

    record_changed
  end

  def apply_sub_record_updates(row, ao_json, subrecord_updates_by_index)
    record_changed = false

    # apply subrecords to the json
    #  - update existing
    #  - add new subrecords
    #  - those not updated are deleted
    subrecord_updates_by_index.each do |jsonmodel_property, updates_by_index|
      subrecords_to_apply = []

      updates_by_index.each do |index, subrecord_updates|
        if (existing_subrecord = Array(ao_json[jsonmodel_property.to_s])[index])
          if subrecord_updates.all?{|_, value| value.to_s.empty? }
            if SpreadsheetBulkUpdater.apply_deletes?
              # DELETE!
              record_changed = true
              next
            else
              errors << {
                sheet: SpreadsheetBuilder::SHEET_NAME,
                column: "#{jsonmodel_property}/#{index}",
                row: row.row_number,
                errors: ["Deleting a subrecord is disabled. Use AppConfig[:spreadsheet_bulk_updater_apply_deletes] = true to enable."],
              }
            end
          end

          if subrecord_updates.any?{|property, value| existing_subrecord[property] != value}
            record_changed = true

            if jsonmodel_property.to_s == 'dates'
              apply_date_defaults(subrecord_updates)
            end
          end

          subrecords_to_apply << existing_subrecord.merge(subrecord_updates)
        else
          if subrecord_updates.values.all?{|v| v.to_s.empty? }
            # Nothing to do!
            next
          end

          if jsonmodel_property.to_s == 'dates'
            apply_date_defaults(subrecord_updates)
          end

          record_changed = true
          subrecord_to_create = SUBRECORD_DEFAULTS.fetch(jsonmodel_property.to_s, {}).merge(subrecord_updates)

          subrecords_to_apply << subrecord_to_create
        end
      end

      ao_json[jsonmodel_property.to_s] = subrecords_to_apply
    end

    record_changed
  end

  def delete_empty_notes(ao_json)
    record_changed = false

    # drop any multipart notes with only empty sub notes
    # - drop subnotes empty note_text
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

    record_changed
  end

  def apply_instance_updates(row, ao_json, instance_updates_by_index)
    record_changed = false

    # handle instance updates
    existing_sub_container_instances = ao_json.instances.select{|instance| instance['instance_type'] != 'digital_object'}
    existing_digital_object_instances = ao_json.instances.select{|instance| instance['instance_type'] == 'digital_object'}
    instances_to_apply = []
    instances_changed = []

    instance_updates_by_index.each do |index, instance_updates|
      if (existing_subrecord = existing_sub_container_instances.fetch(index, false))
        if instance_updates.all?{|_, value| value.to_s.empty? }
          if SpreadsheetBulkUpdater.apply_deletes?
            # DELETE!
            record_changed = true
            instances_changed = true
          else
            errors << {
              sheet: SpreadsheetBuilder::SHEET_NAME,
              column: "instances/#{index}",
              row: row.row_number,
              errors: ["Deleting an instance is disabled. Use AppConfig[:spreadsheet_bulk_updater_apply_deletes] = true to enable."],
            }
          end

          next
        end

        instance_changed = false

        # instance fields
        INSTANCE_FIELD_MAPPINGS.each do |instance_field, spreadsheet_field|
          if existing_subrecord[instance_field] != instance_updates[spreadsheet_field]
            instance_changed = true
            existing_subrecord[instance_field] = instance_updates[spreadsheet_field]
          end
        end

        # sub_container fields
        SUB_CONTAINER_FIELD_MAPPINGS.each do |sub_container_field, spreadsheet_field|
          if existing_subrecord.fetch('sub_container')[sub_container_field] != instance_updates[spreadsheet_field]
            existing_subrecord.fetch('sub_container')[sub_container_field] = instance_updates[spreadsheet_field]
            instance_changed = true
          end
        end

        # the top container
        candidate_top_container = TopContainerCandidate.new(instance_updates['top_container_type'],
                                                            instance_updates['top_container_indicator'],
                                                            instance_updates['top_container_barcode'])

        if candidate_top_container.empty?
          # assume this was intentional and let validation do its thing
          existing_subrecord['sub_container']['top_container']['ref'] = nil
        else
          if @top_containers_in_resource.has_key?(candidate_top_container)
            top_container_uri = @top_containers_in_resource.fetch(candidate_top_container)

            if existing_subrecord.fetch('sub_container').fetch('top_container').fetch('ref') != top_container_uri
              existing_subrecord['sub_container']['top_container']['ref'] = top_container_uri
              instance_changed = true
            end
          else
            errors << {
              sheet: SpreadsheetBuilder::SHEET_NAME,
              column: "instances/#{index}/top_container_indicator",
              row: row.row_number,
              errors: [SpreadsheetBulkUpdater.missing_container_error(candidate_top_container)],
            }
          end
        end

        # did anything change?
        if instance_changed
          record_changed = true
          instances_changed = true
        end

        # ready to apply
        instances_to_apply << existing_subrecord
      else
        if instance_updates.values.all?{|v| v.to_s.empty? }
          # Nothing to do!
          next
        end

        record_changed = true
        instances_changed = true

        instance_to_create = SUBRECORD_DEFAULTS.fetch('instance').merge(
          INSTANCE_FIELD_MAPPINGS.map{|target_field, spreadsheet_field| [target_field, instance_updates[spreadsheet_field]]}.to_h
        )

        instance_to_create['sub_container'].merge!(
          SUB_CONTAINER_FIELD_MAPPINGS.map{|target_field, spreadsheet_field| [target_field, instance_updates[spreadsheet_field]]}.to_h
        )

        candidate_top_container = TopContainerCandidate.new(instance_updates['top_container_type'],
                                                            instance_updates['top_container_indicator'],
                                                            instance_updates['top_container_barcode'])

        if @top_containers_in_resource.has_key?(candidate_top_container)
          top_container_uri = @top_containers_in_resource.fetch(candidate_top_container)
          instance_to_create['sub_container']['top_container'] = {'ref' => top_container_uri}
        else
          errors << {
            sheet: SpreadsheetBuilder::SHEET_NAME,
            column: "instances/#{index}/top_container_indicator",
            row: row.row_number,
            errors: [SpreadsheetBulkUpdater.missing_container_error(candidate_top_container)],
          }
        end

        instances_to_apply << instance_to_create
      end
    end

    if instances_changed
      ao_json.instances = instances_to_apply + existing_digital_object_instances
    end

    record_changed
  end

  def apply_date_defaults(subrecord)
    if subrecord['end'].nil?
      subrecord['date_type'] = 'single'
    else
      subrecord['date_type'] = 'inclusive'
    end

    subrecord
  end

  def extract_columns(filename)
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

  def extract_ao_ids(filename)
    result = []
    each_row(filename) do |row|
      next if row.empty?
      result << Integer(row.fetch('id'))
    end
    result
  end

  TopContainerCandidate = Struct.new(:top_container_type, :top_container_indicator, :top_container_barcode) do
    def empty?
      top_container_type.nil? && top_container_indicator.nil? && top_container_barcode.nil?
    end

    def to_s
      "#<SpreadsheetBulkUpdater::TopContainerCandidate #{self.to_h.inspect}>"
    end

    def inspect
      to_s
    end
  end

  def create_missing_top_containers(in_sheet, job)
    (in_sheet.keys - @top_containers_in_resource.keys).each do |candidate_to_create|
      tc_json = JSONModel::JSONModel(:top_container).new
      tc_json.indicator = candidate_to_create.top_container_indicator
      tc_json.type = candidate_to_create.top_container_type
      tc_json.barcode = candidate_to_create.top_container_barcode

      job.write_output("Creating top container for type: #{candidate_to_create.top_container_type} indicator: #{candidate_to_create.top_container_indicator}")

      tc = TopContainer.create_from_json(tc_json)

      @top_containers_in_resource[candidate_to_create] = tc.uri
    end
  end

  def extract_top_containers_from_sheet(filename, column_by_path)
    top_containers = {}
    top_container_columns = {}

    column_by_path.each do |path, column|
      if [:top_container_type, :top_container_indicator, :top_container_barcode].include?(column.name)
        top_container_columns[path] = column
      end
    end

    each_row(filename) do |row|
      next if row.empty?
      by_index = {}
      top_container_columns.each do |path, column|
        by_index[column.index] ||= TopContainerCandidate.new
        by_index[column.index][column.name] = column.sanitise_incoming_value(row.fetch(path))
      end

      by_index.values.reject(&:empty?).each do |top_container|
        top_containers[top_container] = nil
      end
    end

    top_containers
  end

  def extract_top_containers_for_resource(db, resource_id)
    result = {}

    db[:instance]
      .join(:sub_container, Sequel.qualify(:sub_container, :instance_id) => Sequel.qualify(:instance, :id))
      .join(:top_container_link_rlshp, Sequel.qualify(:top_container_link_rlshp, :sub_container_id) => Sequel.qualify(:sub_container, :id))
      .join(:top_container, Sequel.qualify(:top_container, :id) => Sequel.qualify(:top_container_link_rlshp, :top_container_id))
      .join(:archival_object, Sequel.qualify(:archival_object, :id) => Sequel.qualify(:instance, :archival_object_id))
      .filter(Sequel.qualify(:archival_object, :root_record_id) => resource_id)
      .select(Sequel.as(Sequel.qualify(:top_container, :id), :top_container_id),
              Sequel.as(Sequel.qualify(:top_container, :repo_id), :repo_id),
              Sequel.as(Sequel.qualify(:top_container, :type_id), :top_container_type_id),
              Sequel.as(Sequel.qualify(:top_container, :indicator), :top_container_indicator),
              Sequel.as(Sequel.qualify(:top_container, :barcode), :top_container_barcode))
      .each do |row|
        tc = TopContainerCandidate.new
        tc.top_container_type = BackendEnumSource.value_for_id('container_type', row[:top_container_type_id])
        tc.top_container_indicator = row[:top_container_indicator]
        tc.top_container_barcode = row[:top_container_barcode]

        result[tc] = JSONModel::JSONModel(:top_container).uri_for(row[:top_container_id], :repo_id => row[:repo_id])
    end

    result
  end

  def resource_ids_in_play(filename)
    ao_ids = extract_ao_ids(filename)

    ArchivalObject
      .filter(:id => ao_ids)
      .select(:root_record_id)
      .distinct(:root_record_id)
      .map{|row| row[:root_record_id]}
  end

  def check_sheet(filename)
    errors = []

    # Check AOs exist
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

    # Check AOs all from same resource
    resource_ids = resource_ids_in_play(filename)

    if resource_ids.length > 1
      errors << {
        sheet: SpreadsheetBuilder::SHEET_NAME,
        row: 'N/A',
        column: 'id',
        errors: ["Archival Objects must all belong to the same resource."]
      }
    end

    if errors.length > 0
      raise SpreadsheetBulkUpdateFailed.new(errors)
    end
  end

  def batch_rows(filename)
    to_enum(:each_row, filename).each_slice(BATCH_SIZE) do |batch|
      yield batch
    end
  end

  def each_row(filename)
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

  def row_values(row)
    row.map {|value|
      if value.nil?
        value
      elsif value.is_a?(String)
        result = value.strip
        result.empty? ? nil : result
      else
        # retain type int, date, time, etc
        value
      end
    }
  end

  def self.apply_deletes?
    AppConfig.has_key?(:spreadsheet_bulk_updater_apply_deletes) && AppConfig[:spreadsheet_bulk_updater_apply_deletes] == true
  end

  def create_missing_top_containers?
    if @job.job.has_key?('create_missing_top_containers')
      @job.job['create_missing_top_containers']
    elsif AppConfig.has_key?(:spreadsheet_bulk_updater_create_missing_top_containers)
      AppConfig[:spreadsheet_bulk_updater_create_missing_top_containers]
    else
      false
    end
  end

  def self.missing_container_error(container)
    "Top container not found attached within resource: #{container.inspect}\n" +
      "        *** Set 'Create Missing Top Containers' to create missing Top Containers instead of seeing this error. ***"
  end

end
