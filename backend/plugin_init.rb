unless AppConfig[:plugins].include?('digitization_work_order')
  raise "Hi there, thanks for trying out this great plugin! Currently it relies on the digitization_work_order plugin, which you can download here: https://github.com/hudmol/digitization_work_order -- thanks again!"
end

require_relative '../lib/xlsx_streaming_reader/lib/xlsx_streaming_reader'