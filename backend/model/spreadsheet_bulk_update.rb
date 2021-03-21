require 'write_xlsx'

class SpreadsheetBulkUpdate

  def initialize(resource_uri, ao_uris)
    @resource_uri = resource_uri
    @resource_id = JSONModel.parse_reference(@resource_uri).fetch(:id)
    @ao_uris = ao_uris
  end

  def build_filename
    "bulk_update.resource_#{@resource_id}.#{Date.today.iso8601}.xlsx"
  end

  def to_stream
    io = StringIO.new
    wb = WriteXLSX.new(io)

    sheet = wb.add_worksheet('Updates')

    wb.close
    io.string
  end
end