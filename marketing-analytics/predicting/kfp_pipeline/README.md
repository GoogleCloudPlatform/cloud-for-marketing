
The purpose of this demo is to showcase KFP with Vertex Pipelines.  The pipeline follows these steps:
1) Build a BigQuery view.  This will copy SQL from a GCS bucket, and add parameters
2) Generate a BQML Logistic Regression Model --> Then review performance and save an image of precision/recall to GCS
3) Generate a BQML XGBoost Model --> Then review performance and save an image of precision/recall to GCS
4) Generate a BQML AutoML Model --> Then review performance and save an image of precision/recall to GCS
5) Generate an XGBoost model using XGB directly.  The only purpose of me doing this is to showcase alternative approaches. 
6) Push the XGBoost model to a Vertex AI Endpoint. 