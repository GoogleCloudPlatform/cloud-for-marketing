# Future-Customer-Value-Segments Dataflow pipeline

## Installation
Note: This solution requires Python 3.4 or higher.

* Open Cloud Shell inside the Google Cloud Platform
* Set the project where to install the solution by running: `gcloud config set project [PROJECT_ID]`
* Clone this repo and `cd` into the directory
* Create python3 virtual env `virtualenv env`
* Activate the virtual env `source env/bin/activate`
* Install the project requirements `pip install -r requirements.txt`
* Create (if doesn't exist yet) a bucket where the dataflow template will be stored
* Set an environment variable with the name of the bucket `export PIPELINE_BUCKET=bucket_to_store_template`
* Generate the template by running `./generate_template.sh`
* Move template metadata to the same folder of the template `gsutil cp Future-Customer-Value-Segments_metadata gs://${PIPELINE_BUCKET}/templates`
* Deactivate the Python virtual env at the end `deactivate`
* Close Cloud Shell

## Usage
* Go to the Cloud Dataflow page
* Click `+ Create Job From Template`
* Give the job a name and select `Custom Template` under `Cloud Dataflow template`
* Insert the Template GCS path (`<your_pipeline_bucket_name>/templates/Future-Customer-Value-Segments`)
* Fill the Required Parameters
* Expand the "Optional Parameters" section if needed