class ArchivesSpaceService < Sinatra::Base

  Endpoint.post('/plugins/spreadsheet_bulk_updater/repositories/:repo_id/generate_spreadsheet')
    .description("Return XLSX")
    .params(["repo_id", :repo_id],
            ["uri", [String], "The uris of the records to include in the report"],
            ["resource_uri", String, "The resource URI"])
    .permissions([:view_repository])
    .returns([200, "spreadsheet"]) \
  do
    spreadsheet = SpreadsheetBulkUpdate.new(params[:resource_uri], params[:uri])

    [
      200,
      {
        "Content-Type" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition" => "attachment; filename=\"#{spreadsheet.build_filename}\""
      },
      spreadsheet.to_stream
    ]
  end

end