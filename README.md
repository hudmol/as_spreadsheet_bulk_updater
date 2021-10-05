# as_spreadsheet_bulk_updater

An ArchivesSpace plugin for performing bulk updates on Archival Objects.

Developed by Hudson Molonglo for The New School.

This plugin is compatible with ArchivesSpace v2.8.1.

## Overview

This plugin adds the ability to download a spreadsheet for a set of archival
objects from a particular resource.  This spreadsheet can then be used to apply
a series of changes to those archival objects and the fields presented.  Once
prepared, the changes within the spreadsheet can be applied via a new background
job.

To download the spreadsheet there is now a "Bulk Update Spreadsheet" menu item
available on the resource view or edit page under the "More" menu. You
will be required to select the archival objects to be included in the
spreadsheet and can download the XLSX from this form.

The new background job is called "Spreadsheet Bulk Update Job" and is available
from the "Create > Background Job" menu. To queue your spreadsheet for import,
simply select your XLSX file in the presented form and click "Start Job". 

### Fields and Subrecords

Currently the following Archival Object fields and subrecords are exposed in the
XLSX spreadsheet.

Archival Object fields:
* title
* level

Subrecords:
* dates
* extents
* instances

The spreadsheet contains grouped columns for each existing subrecord plus at
least 3 empty sets to allow creation of new subrecords on the archival object.
The columns provide access to a subset of the subrecord fields, whereby any
mandatory fields not provided are given default values upon import.

If the values provided do not map to an existing subrecord, a new subrecord will
be created.

Notes:
* accessrestrict
* bioghist
* scopecontent

The spreadsheet exposes the first and only first `note_text` for each of the
existing note records for the note types above. Restriction fields for the note
type `accessrestrict` are also exposed.

Minimally 2 sets of columns for each note type are provided. Similarly for
subrecords, if values for a note at an index do not map to an existing note, a
new note will be created.

## Prerequisites

This plugin relies on the `digitization_work_order` plugin being enabled,
available here:

    https://github.com/hudmol/digitization_work_order.

A MySQL database is also required.  For instructions on how to setup MySQL and
ArchivesSpace please see:

    https://archivesspace.github.io/tech-docs/provisioning/mysql.html

## Installation

Download the latest release from the Releases tab in Github:

    https://github.com/hudmol/as_spreadsheet_bulk_updater/releases

Unzip the release and move it to:

    /path/to/archivesspace/plugins

Unzip it:

    $ cd /path/to/archivesspace/plugins
    $ unzip as_spreadsheet_bulk_updater-vX.X.zip
    $ mv as_spreadsheet_bulk_updater-vX.X as_spreadsheet_bulk_updater

Enable the plugin by editing the file in `config/config.rb`:

    AppConfig[:plugins] = ['some_plugin', 'as_spreadsheet_bulk_updater']

(Make sure you uncomment this line (i.e., remove the leading '#' if present))

Install dependencies by initializing the plugin:

    $ cd /path/to/archivesspace
    $ ./scripts/initialize-plugin.sh as_spreadsheet_bulk_updater

See also:

    https://archivesspace.github.io/tech-docs/customization/plugins.html

## Configuration

> AppConfig[:spreadsheet_bulk_updater_apply_deletes] : `boolean`

If enabled, the importer will drop subrecords (dates, extents, instances or
notes) when all spreadsheet columns for that existing subrecord have no values.
As not all subrecord fields have a corresponding column in the spreadsheet, you
may unwittingly drop a subrecord which has data in other fields.

Default: `false`

> AppConfig[:spreadsheet_bulk_updater_create_missing_top_containers] : `boolean`

By default, the importer will throw an error when it finds a top container
in the spreadsheet that is not attached within the current resource's hierarchy.

When enabled, those missing top containers are created on demand.

This configuration setting can be overridden by providing a
`create_missing_top_containers` parameter to the import job.

Default: `false`

## Enhancement Ideas

Here are some thoughts about what could be added in future versions:

-  Export more fields in the spreadsheet and make it configurable
-  Support adding and deleting rows
-  Provide some control over the rules for how many subrecords are exported
-  Provide a direct link to create the import job
-  Provide a way to soft lock the resource to stop others editing it while you fiddle with the spreadsheet
-  Think about rationalizing the dependency on the digitization work order plugin
-  Fix bugs! We know you're in there


