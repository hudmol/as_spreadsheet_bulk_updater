class ArchivesSpaceService < Sinatra::Base

  Endpoint.post('/plugins/spreadsheet_bulk_updater/repositories/:repo_id/generate_spreadsheet')
    .description("Return XLSX")
    .params(["repo_id", :repo_id],
            ["uri", [String], "The uris of the records to include in the report"],
            ["min_subrecords", Integer, "The minimum number of subrecords to include", :default => 0],
            ["extra_subrecords", Integer, "The number of extra subrecords to include", :default => 3],
            ["min_notes", Integer, "The minimum number of note subrecords to include", :default => 2],
            ["resource_uri", String, "The resource URI"],
            ["selected_columns", [String], "The set of columns to include"],
           )
    .permissions([:view_repository])
    .returns([200, "spreadsheet"]) \
  do
    builder = SpreadsheetBuilder.new(params[:resource_uri],
                                     params[:uri],
                                     params[:min_subrecords],
                                     params[:extra_subrecords],
                                     params[:min_notes],
                                     params[:selected_columns])

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
