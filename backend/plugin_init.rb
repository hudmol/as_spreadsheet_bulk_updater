if ASpaceEnvironment.demo_db?
  msg = [
    "Hi there, thanks for trying out this great plugin!",
    "Currently it requires a MySQL database and sadly won't work with the demo Derby database."
  ]
  Log.error("\n\nWe hit an error while starting as_spreadsheet_bulk_updater:\n\n" + msg.join("\n") + "\n\n")
  raise msg.join(' ')
end

require_relative '../lib/xlsx_streaming_reader/lib/xlsx_streaming_reader'
