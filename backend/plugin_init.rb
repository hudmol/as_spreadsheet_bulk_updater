unless AppConfig[:plugins].include?('digitization_work_order')
  raise "Hi there, thanks for trying out this great plugin! Currently it relies on the digitization_work_order plugin, which you can download here: https://github.com/hudmol/digitization_work_order -- thanks again!"
end

if ASpaceEnvironment.demo_db?
  raise "Hi there, thanks for trying out this great plugin! Currently it requires a MySQL database and sadly won't work with the demo Derby database."
end

require_relative '../lib/xlsx_streaming_reader/lib/xlsx_streaming_reader'