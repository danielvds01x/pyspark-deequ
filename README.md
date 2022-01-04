# pyspark-pydeequ

## Usage

An way to call this module would be:

```bash
gcloud dataproc jobs submit pyspark # submit job to dataproc
data_quality_pyspark.py # this module
--project my-gcp-project # gcp project id
--cluster my-spark-cluster # cluster
--region us-central1 # region
-- gcs # source type (bq | gcs)
gs://project-bucket/path/to/file.csv | .parquet
DATA_PROFILING.ANALYSIS_RESULTS # Bigquery table where will be saved the results
"col1 = 'a' or|and col2 = 'b'" # bq table filter optional, like a sql where clause
```

## License
[Apache 2.0](LICENSE.md)

## Link to PyDeequ project source
[https://github.com/awslabs/python-deequ](https://github.com/awslabs/python-deequ)
