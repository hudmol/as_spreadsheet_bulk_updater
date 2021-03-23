class ArchivesSpaceService < Sinatra::Base

  Endpoint.post('/plugins/spreadsheet_bulk_updater/repositories/:repo_id/generate_spreadsheet')
    .description("Return XLSX")
    .params(["repo_id", :repo_id],
            ["uri", [String], "The uris of the records to include in the report"],
            ["resource_uri", String, "The resource URI"])
    .permissions([:view_repository])
    .returns([200, "spreadsheet"]) \
  do
    builder = SpreadsheetBuilder.new(params[:resource_uri], params[:uri])

    [
      200,
      {
        "Content-Type" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition" => "attachment; filename=\"#{builder.build_filename}\""
      },
      builder.to_stream
    ]
  end

end