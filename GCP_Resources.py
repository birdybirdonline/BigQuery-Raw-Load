from google.cloud import bigquery, storage

# when run from Google Cloud Console,
# credentials + project definition are implied


class GCP_Resources:
    def __init__(self):
        #GCP resource clients
        self.bq = bigquery.Client()
        self.storage = storage.Client()

        # define buckets here

        # for fully qualified bucket URI starting "gs://"
        self.fq_bucket = "gs://bucket"

        #TODO: the ingress bucket name alone, no trailing /
        self.ingress_bucket = "bucket"

        #TODO: for folders within buckets, define the folder name
        # in the format foldername+"/"
        self.bucket_prefix = "foldername/"

        #TODO: define a destination dataset. No project name required
        # Additional refactoring will be required if you need to use multiple
        # destination datasets.
        self.bq_dataset = "dataset"
        
GCP = GCP_Resources()