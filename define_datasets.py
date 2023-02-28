# Manually define the raw datasets'expected filenames to be 
# found in cloud storage.
# Also define a dict to associate a given list of date fields to parse in pandas.
# If datasets names or date fields change, this script must be updated

"""Define a list of expected dataset names that are expected to be found in Cloud
Storage, and a dict that defines the date fields of the given dataset in order to parse
them from CSV using pandas.read_csv() parse_dates parameter"""
#TODO: as with BigQueryTransfer.py, assumption is that you're working with CSVs
# refactor to your requirements.


# expected raw dataset names, named according to our desired destination tables also
Example_Dataset_0 = "Example_Dataset_0.csv"
Example_Dataset_1 = "Example_Dataset_2.csv"
Example_Dataset_2 = "Example_Dataset_3.csv"

expected_datasets = [
    Example_Dataset_0,
    Example_Dataset_1,
    Example_Dataset_2
]

# define dataset and table for ingestion as dict entries
# value for each key is list of date fields that pandas must parse first
# #TODO: maybe not necessary if not using CSV/JSON; could also use this for passing a schema instead,
# with more refactoring. However it may be better to keep JSONs of your schemas in a bucket of their own
# and passing these to the main script to validate against each accordingly
datasets_ctrl_dict = {
    Example_Dataset_0: ["date_column_0", "date_column_1", "ingress_date"],
    Example_Dataset_1: ["date_column_0", "date_column_1", "date_column_2" "ingress_date"],
    Example_Dataset_2: ["date_column_0", "ingress_date"]
}
