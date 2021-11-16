
The purpose of this demo is to showcase KFP with Vertex Pipelines.  The pipeline follows these steps:
1) Build a BigQuery view.  This will copy SQL from a GCS bucket, and add parameters
2) Generate a BQML Logistic Regression Model --> Then review performance and save an image of precision/recall to GCS
3) Generate a BQML XGBoost Model --> Then review performance and save an image of precision/recall to GCS
4) Generate a BQML AutoML Model --> Then review performance and save an image of precision/recall to GCS
5) Generate an XGBoost model using XGB directly.  The only purpose of me doing this is to showcase alternative approaches. 
6) Push the XGBoost model to a Vertex AI Endpoint. 

In order to make this run, a few requirements or recommendations:
1) Using GCP's Vertex AI Workbench (Notebook Offering) will be the easiest way to run this code.  Otherwise, you will need to explicitly authenticate
2) You will need to have access to or build a GCS Bucket
3) You will need Google BigQuery. This will build a BigQuery View and 3 BigQuery Machine Learning models.
4) This will also build a GCP Function, and schedule it with GCP Scheduler.  
5) This will also build a model that gets pushed to an endpoint.