ArchivesSpace::Application.routes.draw do

  match('/plugins/spreadsheet_bulk_updater/download' => 'spreadsheet_bulk_updater#download_form', :via => [:get])
  match('/plugins/spreadsheet_bulk_updater/download' => 'spreadsheet_bulk_updater#download', :via => [:post])

end
