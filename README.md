# as_spreadsheet_bulk_updater

An ArchivesSpace plugin for performing bulk updates on Archival Objects.

Developed by Hudson Molonglo for The New School.

## Overview

This plugin adds the ability to download a spreadsheet for a set of archival
objects from a particular resource.  This spreadsheet can then be used to apply
a series of changes to those archival objects and the fields presented.  Once
prepared, the spreadsheet can be applied via a new background job.

To download the spreadsheet there is now a "Bulk Update Spreadsheet" menu item
available on the resource view or edit page menu (under the "More" menu). You
will be required to select the archival objects to be included in the
spreadsheet and can download the XLSX from this form.

The new background job is called "Spreadsheet Bulk Update Job" and is available
from the "Create > Background Job" menu. To queue your spreadsheet for import,
simply select your XLSX file in the presented form and click "Start Job". 

## Prerequisites

This plugin relies on the digitization_work_order plugin being enabled,
available here: https://github.com/hudmol/digitization_work_order.

## Installation

Download the latest release from the Releases tab in Github:

  https://github.com/hudmol/as_spreadsheet_bulk_updater/releases

Unzip the release and move it to:

    /path/to/archivesspace/plugins

Unzip it:

    $ cd /path/to/archivesspace/plugins
    $ unzip as_spreadsheet_bulk_updater-vX.X.zip

Enable the plugin by editing the file in `config/config.rb`:

    AppConfig[:plugins] = ['some_plugin', 'as_spreadsheet_bulk_updater']

(Make sure you uncomment this line (i.e., remove the leading '#' if present))

Install dependencies by initializing the plugin:

    $ cd /path/to/archivesspace
    $ ./scripts/initialize-plugin.sh as_spreadsheet_bulk_updater

See also:

  https://archivesspace.github.io/archivesspace/user/archivesspace-plug-ins/
