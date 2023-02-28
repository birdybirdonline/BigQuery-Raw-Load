from google.cloud import bigquery
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import os
import traceback
from GCP_Resources import GCP
from define_datasets import expected_datasets, datasets_ctrl_dict

# ASSUMES YOU'RE WORKING WITH CSV. MAY REQUIRE ADJUSTMENT
# IF USING OTHER FORMATS - marked with #TODO; please review all
# TODOs when changing format, partition types etc

class Prepare_Job:
    """instantiate an object to hold
    all the job details so that each 
    job can be passed to the loader individually,
    one at a time and without ambiguity
    """

    def __init__(self, GCP, dataset):

        # assigning the uri
        self.csv_uri = GCP.fq_bucket + dataset

        # assumes that you want the bq table to be
        # named the same as its source file, stripping
        # the file extension
        self.bq_table = os.path.splitext(dataset)[0]

        # if using CSV or JSON, must ensure date fields are parsed
        # by pandas to UTC to maintain schema
        try:
            print(f'# Parsing dates from {dataset} into dataframe...')
            
            # parse dates are defined as a dict entry in the define_datasets.py module
            parse_dates = datasets_ctrl_dict[dataset]

            #TODO: pd.read_csv should be changed to match the appropriate input type
            self.df = pd.read_csv(self.csv_uri, parse_dates=parse_dates)
        
        # Note: bare exceptions throughout script; to be tuned accordingly. WIP
        # using traceback as bandaid
        except Exception:
            print(f"# Failure to parse dates into dataframe for {dataset}")
            print(traceback.format_exc()) 

        # configure the load job for BigQuery with necessary params
        try:
            self.job_config = bigquery.LoadJobConfig(

                # TODO: - autodetecting schema. Change this if you're
                # going to manually define schema via the schema= param.
                autodetect=True,
                schema_update_options = [
                    # allow the addition of new fields rather than failing on schema mismatch.
                    # If your raw source data changes format over time, you can also use .ALLOW_FIELD_UPDATE
                    # To accomodate changes to existing fields
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],

                # TODO: this line asserts source format as CSV, so change if other format
                source_format = bigquery.SourceFormat.CSV,

                # TODO: assumes you want partitioning by month
                # and uses a field from the csv to define this date -
                # 'ingress_date' is the example for the template.
                time_partitioning = bigquery.TimePartitioning(
                    field='ingress_date',
                    type_=bigquery.TimePartitioningType.MONTH
                ),
                #TODO: write disposition tells bq how to treat the data & table; WRITE_APPEND
                # will create the table if it doesn't exist, or append the data if it does.
                # WRITE_APPEND is our default for templating but you should investigate.
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            )

            # TODO: Some datasets may have long string fields broken by new lines; setting
            # this param to True will enable bigquery to parse this appropriately.
            # Not necessary if your dataset doesn't have long string fields with newlines
            self.job_config.allow_quoted_newlines = True

            # TODO: this will help with parsing rows that have null values in some columns
            self.job_config.allow_jagged_rows = True

        except Exception:
            print("# Error while configuring load job.")
            print(traceback.format_exc())
        
def init_load():
    """initialize, first checking that all required datasets are present,
    then pass each iteratively through Load()"""

    # Check for presence of all required datasets
    print("# Checking for presence of required atasets in GCS bucket...")
    
    # iterate over the storage bucket to list present files; this is useful
    # if you want to enforce that the script does not progress unless all datasets
    # are present. See next #TODO for more info if you want to permit progress
    # without a required list. Prefix can be omitted if not using folders within buckets.
    bucket = GCP.storage.get_bucket(GCP.ingress_bucket)
    present_in_bucketlist = bucket.list_blobs(prefix=GCP.ingress_prefix)
    blob_names = {blob.name for blob in present_in_bucketlist}

    missing_datasets = []

    for dataset in expected_datsets:
        # if not using folders within buckets, can just use dataset name 
        # without prefix
        if f"{GCP.ingress_prefix}{dataset}" not in blob_names:
            missing_datasets.append(dataset)

    # report missing datasets to user

    #TODO: template assumes you would want to stop if any files are missing.
    # refactor this code block if you want to enable continuing to load data
    # even if one or more sets are missing. Next TODO also relevant.
    if missing_datasets:
        print("# The following datasets are missing from the ingress bucket:")
        for i, dataset in enumerate(missing_datasets):
            print(f"{dataset}")
            print("# Please ensure these files are present and named correctly before reattempting.")
    
    # else prepare each dataset for loading
    else:
        jobs_dict = {}
        print(f"# Preparing dataset load jobs:")
        for i, dataset in enumerate(expected_datasets):
            print(f"# {i+1} of {len(expected_datasets)}: {dataset}")
            jobs_dict[f"{dataset}_job"] = dataset, Prepare_Job(GCP, dataset)

    # attempt each load job in turn
    for job in jobs_dict.values():
        try:
            # as above, jobs_dict entries
            # are defined with a tuple - the dataset, and the job itself
            job_name = job[0]
            job = job[1]
        
            # step through each job with user confirming load;
            # enables user to restart job, selecting a start point by skipping sets
            # TODO: refactoring also required here if you want to remove the 
            # enforcement of a required list of datasets
            while True:
                command = input(f"# Attempt to Load {job_name}? Y/N: ").lower()
                if command == "y":
                    print(f"# Loading {job_name}")
                    load(job)
                    print(f"# {job_name} Loaded Successfully")
                elif command == "n":
                    print(f"# Skipping {job_name}...")
                    break
                else: 
                    print(f"Invalid command '{command}'")
        except Exception:
            print(f"# Failure to load {job_name}")
            print(traceback.format_exc())
        print("# Complete: All selected datasets successfully loaded.")
        return

def load(job):
    """Function to actually pass each defined job into
    BigQuery client to load the data into the defined tables in
    the given dataset."""

    # construct table reference for loading. Assumes you want to name
    # the table after the raw dataset, sans the file extension.
    bq_table_ref = GCP.bq.dataset(f"{GCP.bq_dataset}").table(jobs.bq_table)

    # load job
    load_job = GCP.bq.load_table_from_dataframe(
        job.df, bq_table_ref, job_config=job.job_config
    )
    load_job.result()
    return

if __name__ == "__main__":
    init_load()