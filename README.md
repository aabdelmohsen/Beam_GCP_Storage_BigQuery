# Beam_GCP_Storage_BigQuery
This is an example of how to read a CSV using Apache Commons inside Beam pipeline and write it inside Big Query tables


## Steps to run the pipeline:-
### First Set Google Credentials
export GOOGLE_APPLICATION_CREDENTIALS="<Your Service Account Json Key Path>"
  
### Pipeline Local Run Command
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.NeighbourhoodPipeline -Dexec.args="--output=gs://springml_gcp_bucket/newyork-airbnb/output --tempLocation=gs://springml_gcp_bucket/temp/" -Pdirect-runner


### Pipeline Dataflow Run commad
java -jar target/word-count-beam-bundled-0.1.jar  --runner=DataflowRunner   --project=newyork-airbnb --region=us-central1 --tempLocation=gs://springml_gcp_bucket/temp/ --output=gs://springml_gcp_bucket/newyork-airbnb/output
