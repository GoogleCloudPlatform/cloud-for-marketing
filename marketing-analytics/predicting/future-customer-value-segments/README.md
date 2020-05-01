# Future-Customer-Value-Segments (FoCVS) Cloud Dataflow pipeline

## Overview

Future-Customer-Value-Segments (aka FoCVS) is a data-processing pipeline that helps understand users behavior by calculating Customer Lifetime Value and segmenting customers by total value.

It runs on Google Cloud Dataflow and can be deployed to any GCP account.

There are two different versions of the pipeline: 
* CSV version (read input data and store output as CSV files in Cloud Storage)
* BigQuery version (read input data and store output in BigQuery tables)

## Goals

1. Let the users run the pipeline themselves, as many time as they want, using different input data or different parameters.
2. First focus on privacy; input transactions data doesn't need to be shared with Google or any other company.

## How to use the solution

The solution consists of Cloud Dataflow templates that can be run using different runtime parameters to customize the execution of the pipeline.

The following procedure explains how to install the Cloud Dataflow template in a Google Cloud Platform project.

Alternatively the user can run the Cloud Dataflow pipeline (see Usage section) by referring to the publicly available templates:

* CSV version: `gs://future-customer-value-segments/templates/FoCVS-csv`
* BigQuery version: `gs://future-customer-value-segments/templates/FoCVS-bq`

### Installation

Note: This solution requires Python 3.7.

* Open Cloud Shell inside the Google Cloud Platform
* Set the project where to install the solution by running: `gcloud config set project [PROJECT_ID]`
* Clone this repo and `cd` into the directory
* Create python3 virtual env `virtualenv env`
* Activate the virtual env `source env/bin/activate`
* Install the project requirements `pip install -r requirements.txt`
* Create (if doesn't exist yet) a bucket where the dataflow template will be stored
* Set an environment variable with the name of the bucket `export PIPELINE_BUCKET=bucket_to_store_template`
* Generate the template by running `./generate_template.sh`
* Move template metadata to the same folder of the template `gsutil cp FoCVS-*_metadata gs://${PIPELINE_BUCKET}/templates`
* Deactivate the Python virtual env at the end `deactivate`
* Close Cloud Shell

### Usage

* Go to the Cloud Dataflow page
* Click `+ Create Job From Template`
* Give the job a name and select `Custom Template` under `Cloud Dataflow template`
* Insert the GCS path of the pipeline template version to use (e.g. for BigQuery version `<your_pipeline_bucket_name>/templates/FoCVS-bq` or the public template `gs://future-customer-value-segments/templates/FoCVS-bq`)
* Fill the Required Parameters
* Expand the "Optional Parameters" section if needed

## Data

### Input Data

The pipeline takes as input data a CSV file (or BigQuery table) containing the transaction data for the customers (the file must contain a header describing the columns). It must contain the following fields:

* **Customer ID** (an identifier for the customer, can be either a number or a string).
* **Date of the transaction** (must be in one of the following formats: 'YYYY-MM-DD’, 'MM/DD/YY', 'MM/DD/YYYY’, 'DD/MM/YY', 'DD/MM/YYYY’, 'YYYYMMDD').
* **Value of the transaction** (number)
* **Extra dimension** *(Optional)* (can be a number or a string referring for example to a category, marketing channel, geographic region or any other property of the transaction).

Here’s an example of input data from the CDNOW popular dataset:
```
customer_id,date,category,sales
00001,1997-01-01,1,11.77
00002,1997-01-12,1,12.00
00002,1997-01-12,5,77.00
00003,1997-01-02,2,20.76
00003,1997-03-30,2,20.76
00003,1997-04-02,2,19.54
```

### Output Data

The Future-Customer-Value-Segments pipeline generates a bunch of files in the output directory.
These files can be divided into two categories, **validation files** and **prediction reports**:

#### Validation Files

***validation_params.txt***

Contains information regarding the validation of the model. See an example below:

```
Modeling Dates
Calibration Start Date: 1997-01-01
Calibration End Date: 1997-10-01
Cohort End Date: 1997-01-31
Holdout End Date: 1998-06-30

Model Time Granularity: Weekly
Frequency Model: BG/NBD

Customers modeled for validation: 7814 (33.25% of total customers)
Transactions observed for validation: 23497 (34.8% of total transactions)

Mean Absolute Percent Error (MAPE): 2.43%
```

***repeat_transactions_over_time.png (and repeat_cumulative_transactions_over_time.png)***

Contains a chart that help the user understand how well the model fits the input data.

The first part of the chart is relative to the calibration period of the model.

#### Prediction Reports

***prediction_params.txt***

Contains information about the prediction period and the parameters used in the model. See an example below:

```
Prediction for: 52 weeks
Model Time Granularity: Weekly

Customers modeled: 23502
Transactions observed: 67511

Frequency Model: BG/NBD
Model Parameters
r: 0.26805064950507473
alpha: 5.991516710871457
a: 0.4901959576295743
b: 2.1372211217801924

Gamma-Gamma Parameters
p: 7.676093116272801
q: 3.6321332914686537
v: 11.435020413203247
```

***Prediction CSV Files (or BigQuery tables)***

* **prediction_summary** &mdash; prediction grouped by segment (useful to understand who are those customers providing the best value)
* **prediction_summary_extra_dimension** &mdash; prediction grouped by extra dimension
* **prediction_by_customer** &mdash; prediction for each single customer

Those files contain the model prediction output data:

* **Retention Probability** (Likelihood of a customer to come back)
* **Predicted Purchases** (Predicted future purchases for the next year)
* **Future AOV** (Predicted future value per order)
* **Expected Value** (Predicted future spend for the next year)
* **Customer Equity** (Summed predicted future spend for customers in segment)
