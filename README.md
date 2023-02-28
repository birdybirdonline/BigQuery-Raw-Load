# Bigquery Raw Data Load Template

This template is designed to assist with loading raw data from .csv files into BigQuery tables,
either creating the tables on the fly or appending to existing tables.

It's designed to more or less work out of the box except that you will need to define the variables in the `define_datasets.py`
script and some of the variables in the `GCP class` in `GCP_Resources.py`.

If you are happy with the template, the main `BigQueryTransfer.py` script should not need any alteration once the above is complete.

The template is marked throughout with #TODOs in key areas to assist with refactoring for
non-CSV source files and other controls.


<br>

## Assumptions

Some basic assumptions are made in the template, but all are adjustable:

1. **You are running the script from within GCP's Cloud Console** - When working inside Cloud Console,
    the destination Project and the necessary user credentials are implied. However, you can include
    an additional codeblock for handling credentials (and the project name) If you prefer to run locally, or if you
    intend to follow best practice and use a service account on GCP instead of your user account.

2. **You are working with CSV files** - The #TODOs will assist with refactoring to adjust this

3. **You want to define a list of required / expected datasets to pass into BQ** -  #TODOS are marked
    to help refine this if, for example, you want the user to be able to continue even if certain datasets are missing.


<br>

## Requirements

Given assumption **1.**, Cloud Console will already be kitted with the necessary GCP packages for python, so no requirements.txt
is included in this repo. However, you will likely need to `pip3 install pandas`, depending on if Google make any changes to the Cloud
Console after time of writing this README.

<br>

## #TODOs and commenting

These scripts are excessively commented in an effort to give a broader understanding of precisely how the code works
if you are new to GCP or aren't sure about the particular params and methods used. 

**ALL #TODOs** in the scripts are specifically placed to mark areas of the code that you **must** refactor if you wish
to change from the assumptions above, e.g. if you want to use a format other than CSV. 

<br>

## Other Formats

If you don't want to use CSV, you'll need to refactor accordingly. In terms of BQ's requirements for tables, JSON is the most
similar to CSV in that both require parsing dates as **UTC format**, however other formats may not have this limitation.

You may wish to add additional code blocks for parsing excel (you'd probably need to convert to CSV / JSON or other) or other formats.
