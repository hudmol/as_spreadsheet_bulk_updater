ArchivesSpace::Application.routes.draw do
  [AppConfig[:frontend_proxy_prefix], AppConfig[:frontend_prefix]].uniq.each do |prefix|
    scope prefix do
      match('/plugins/spreadsheet_bulk_updater/download' => 'spreadsheet_bulk_updater#download_form', :via => [:get])
      match('/plugins/spreadsheet_bulk_updater/download' => 'spreadsheet_bulk_updater#download', :via => [:post])
    end
  end
end