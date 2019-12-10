<!-- # ML Data Windowing Pipeline -->

The ML Data Windowing Pipeline is a set of Google Cloud Dataflow pipelines, used
to process Google Analytics 360 or CRM data into a Machine Learning (ML)-ready
dataset.

The final dataset created by the pipeline can be used for building ML models
such as Propensity Models, Response Models, Recommender Systems, Customer
Lifetime Models, etc.

The resulting dataset will contain snapshots of dates sliding at set intervals.
Each date contains a user’s features based on what has happened in the past (a
set lookback window respective of the date to aggregate data in), and a label
based on what will happen in the future (a set prediction window respective of
the date).

We’ve built this for you to help improve the quality of the dataset used in
these models, and streamline the process.

## Quickstart

You can use Google Cloud Build to generate Templates for the entire pipeline
without installing any development tools.

1.  Be sure you are logged into the correct Google Cloud account in your
    console.

2.  [Create a GCS Bucket](https://cloud.google.com/storage/docs/creating-buckets)
    to hold temporary files and the generated templates.

3.  Navigate to the root directory of the project, and run the build with:

    ```bash
    gcloud builds submit --config=cloud_build.json --substitutions=_BUCKET_NAME=$MY_BUCKET_NAME
    ```

4.  After you've created the templates, they will be located in the
    `mlwp_templates` folder in your GCS Bucket. You can
    [run them with these instructions](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#custom-templates).

## Initial Setup

1. [Google Cloud SDK](https://cloud.google.com/sdk/install).
2. [Apache Maven](https://maven.apache.org/install.html).
3. Ensure you are using Java 9.
4. See [Quickstart Using Java and Apache Maven](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven)
for information on enabling APIs (importantly enable the Dataflow API).

## Overview

This is a four step process, and accordingly there are four templates available
to use.

This README file documents the relevant Maven -Pdataflow-runner commands for
each step.

The code can be run directly in Google Cloud Dataflow pipelines or built as
Google Cloud Dataflow templates.

### Step 1. UserSessionPipeline.java

The pipeline has been optimised to read data directly from the
* [Google Analytics 360 BigQuery output table](https://support.google.com/analytics/answer/3437618?hl=en)
between a set of specified dates. This stage digests
the Analytics data into AVRO files. The AVRO files will be used in the next
steps.

At this point in the pipeline we specify what the prediction label is (i.e the
value we are trying to predict in our model). The pipeline will look at the
Google Analytics column name and positive value to create this label in the
output.

### Step 2. DataVisualizationPipeline.java

This is an optional - but recommended - step in the pipeline to output data from
Step 1 into BigQuery for exploration before processing it further.

Creating two tables in BigQuery...

* Facts Table. A long list of variables and values for each activity time /
session, based on the original data.

* Instance Table. Containing snapshots of all user
labels - based on a prediction window - sliding at set intervals to create
effective times / sessions.

The Instance table contains snapshot summary of each user's activity from the
start date to each effective date.

    -|-------|-------|-------|-------|-------|-------|-------|   ...  -|-
     d      d+1     d+2     d+3     d+4     d+5     d+6     d+7       d+n

It then moves the effective date by a predefined sliding time. The combination
of a user and effective date defines an instance snapshot.

    -|-------|-------|-------|-------|--- ... ---|-------|-------|
    d-1      d      d+1     d+2     d+3         d+k
             |                                   |
             |________ Prediction Window ________| --> Moved by Slide Time

For each effective date, the pipeline will output all users that have performed
an action before that date along with:

* label within a predefined prediction window (hasPositiveLabel column).
* days between the first available activity and effective date
  (daysSinceStartDate column).
* days since the last available activity and effective date
  (daysSinceLatestActivity column).

Once this dataset is produced, one can run the following operations:

* get an understanding of the size and the label distribution of instances over
  time.
* get an understanding of the data availability of for instances (by analysing
  days since first and days since last information) and define a meaningful
  lookback window.
* filter the instances based on label imbalance/values or by days since last
  to select active instances for model building.

### Step 3. SlidingWindowPipeline.java or SessionBasedWindowPipeline.java

SlidingWindowPipeline.java
Using the AVRO files from Step 1 build snapshots of effective dates sliding at
set intervals, windowing the data based on a specified lookback and prediction
window.

In this step the pipeline will - as described for Step 2 - create instance
snapshots of users, effective dates, and labels within a set prediction window.

More importantly, it will window the data based on a set lookback window,
sliding along the timeline of effective dates. Ready to be aggregated into
features.

    -|-------|-------|-------|-------|-------|-------|-------|
    d-h     d-3     d-2     d-1      d      d+1
     |                       |
     |___ Lookback Window ___|

SessionBasedWindowPipeline.java
Instead of sliding windows, this pipeline outputs windowed data for every
session instead of effective date. This pipeline is best when generating
features at the session level.

### Step 4. GenerateFeaturesPipeline.java

Using the AVRO files from Step 3 create features based on the lookback windows
created. The features are based on different types of aggregated events that
happened in the lookback window.

The resulting output will be a BigQuery table containing features for all
snapshots of users, ready for ML training.

## Running the Pipeline

### Step 1. UserSessionPipeline.java

#### Options:

*   Input BigQuery SQL *Required*<br>
    The SQL to select from the Google Analytics table in BigQuery (Note: Ensure
    the pipeline has access to this).<br>
    Note: If useful, you can reduce dataset size early on by using the SQL to
    filter on the relevant date period being considered.<br>
    e.g SELECT * FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`

*   Prediction Fact Name *Required*<br>
    The name of the BigQuery column that contains the value we are trying to
    predict.<br>
    e.g hits.eventInfo.eventAction<br>

*   Prediction Fact Value Name *Required*<br>
    The value in the BigQuery column that represents a positive label of what we
    are trying to predict (can have multiple values separated by commas).<br>
    **Note: This value is read as a string and matched using string.equals**<br>
    e.g Add to Cart<br>

*   Output Sessions AVRO Prefix *Required*<br>
    The location on Google Cloud Storage to output the AVRO files to.<br>
    **Note: The pipeline will not clean up existing AVRO files in the location provided. So it is advisable to specify unique AVRO prefix for every execution of this pipeline to avoid mixing up AVRO files.**<br>
    e.g gs://[GCS BUCKET]/usersession-output/Nov26_01/

#### To run this directly (using the examples from options above):

    $ mvn -Pdataflow-runner compile exec:java@run \
        -Dexec.mainClass=com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.UserSessionPipeline \
        -Dexec.args="--runner=DataflowRunner \
        --project='[CLOUD PROJECT TO RUN IN]' \
        --inputBigQuerySQL='SELECT * FROM \`bigquery-public-data.google_analytics_sample.ga_sessions_*\`' \
        --predictionFactName='hits.eventInfo.eventAction' --predictionFactValues='Add to Cart' \
        --outputSessionsAvroPrefix='gs://[GCS BUCKET]/usersession-output/Nov26_01/'"

Once built, open Dataflow in your Google Cloud Platform project to see the
pipeline running.

#### To build a template:

    $ mvn -Pdataflow-runner compile exec:java \
        -D_MAIN_CLASS=UserSessionPipeline \
        -D_PROJECT_ID=[CLOUD PROJECT TO RUN IN] \
        -D_OUTPUT_LOCATION=gs://[GCS BUCKET]/usersession-template \
        -D_TEMP_LOCATION=gs://[GCS BUCKET]/temp/mlwp_templates

### Step 2. DataVisualizationPipeline.java

#### Options:

    -|-------|-------|-------|-------|--- ... ---|-------|-------|
    d-1      d      d+1     d+2     d+3         d+k
                     |                           |
                     |____ Prediction Window ____| --> Moved by Slide Time
                     |                           |
                     -- Minimum Lookahead Time   |
                                                 -- Maximum Lookahead Time

*   Input AVRO Sessions Location *Required*<br>
    The location of the AVRO files from Step 1.<br>
    e.g gs://[GCS BUCKET]/usersession-output/Nov26_01/*.avro

*   Snapshot Start Date *Required*<br>
    Date of the first snapshot in dd/mm/yyyy format.

*   Snapshot End Date *Required*<br>
    Date of the last possible snapshot (inclusive) in dd/mm/yyyy format.

*   Slide Time In Seconds *Required*<br>
    The time interval - in seconds - to slide snapshot dates by.<br>
    e.g 604800 (for 7 days)

*   Minimum Lookahead Time In Seconds *Required*<br>
    The time - in seconds - for the prediction window to start from, respective of
    the current date.<br>
    e.g 86400 (for the prediction window to start from the next day)

*   Maximum Lookahead Time In Seconds *Required*<br>
    The time - in seconds - for the prediction window to end on, respective of the
    current date. The length of the prediction window being the max - min times.<br>
    e.g 1209600 (would have a prediction window of 14 days)

*   Stop On First Positive Label *Required*<br>
    Stop considering a user once they have a positive label.<br>
    e.g true

*   Output BigQuery Facts Table *Required*<br>
    The location of the BigQuery Facts table (Note: Ensure the pipeline has write
    access to this).<br>
    e.g myproject.mydataset.ga_facts_table
    **Note: the format[project_id]:[dataset_id].[table_id]**

*   Output BigQuery Instance (User Activity) Table *Required*<br>
    The location of the BigQuery Instance table (Note: Ensure the pipeline has
    write access to this).<br>
    e.g myproject.mydataset.ga_instance_table
    **Note: the format[project_id]:[dataset_id].[table_id]**

#### To run this directly (using the examples from options above):

    $ mvn -Pdataflow-runner compile exec:java@run \
        -Dexec.mainClass=com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.DataVisualizationPipeline \
        -Dexec.args="--runner=DataflowRunner \
        --project='[CLOUD PROJECT TO RUN IN]' \
        --inputAvroSessionsLocation='gs://[GCS BUCKET]/usersession-output/Nov26_01/*.avro' \
        --snapshotStartDate='01/01/2017' \
        --snapshotEndDate='01/08/2017' \
        --slideTimeInSeconds='604800' \
        --minimumLookaheadTimeInSeconds='86400' \
        --maximumLookaheadTimeInSeconds='1209600' \
        --stopOnFirstPositiveLabel='true' \
        --outputBigQueryFactsTable='myproject:mydataset.ga_facts_table' \
        --outputBigQueryUserActivityTable='myproject:mydataset.ga_instance_table'"

Once built, open Dataflow in your Google Cloud Platform project to see the
pipeline running.

#### To build a template:

    $ mvn -Pdataflow-runner compile exec:java \
        -D_MAIN_CLASS=DataVisualizationPipeline \
        -D_PROJECT_ID=[CLOUD PROJECT TO RUN IN] \
        -D_OUTPUT_LOCATION=gs://[GCS BUCKET]/datavisualization-template \
        -D_TEMP_LOCATION=gs://[GCS BUCKET]/temp/mlwp_templates

### Step 3. SlidingWindowPipeline.java or SessionBasedWindowPipeline.java

#### Options: SlidingWindowPipeline

    -|--- ... ---|-------|-------|-------|-------|-------|-------|--- ... ---|--
    d-j         d-3     d-2     d-1      d      d+1     d+2     d+3         d+k
     |                           |       |       |                            |
     |_____ Lookback Window _____|       |       |____ Prediction Window _____| --> Moved by Slide Time
                                 |       |       |                            |
                                 |       |       -- Minimum Lookahead Time    |
                                 |_______|                                    -- Maximum Lookahead Time
                                Lookback Gap

*   Input AVRO Sessions Location *Required*<br>
    The location of the AVRO files from Step 1.<br>
    e.g gs://[GCS BUCKET]/usersession-output/Nov26_01/*.avro

*   Start Date *Required*<br>
    Start date to consider data from, in dd/mm/yyyy format.

*   End Date *Required*<br>
    End date (not inclusive) to stop considering data, in dd/mm/yyyy format.

*   Slide Time In Seconds *Required*<br>
    The time interval - in seconds - to slide snapshot dates by.<br>
    e.g 604800 (for 7 days)

*   Minimum Lookahead Time In Seconds *Required*<br>
    The time - in seconds - for the prediction window to start from, respective of
    the current date.<br>
    e.g 86400 (for the prediction window to start from the next day)

*   Maximum Lookahead Time In Seconds *Required*<br>
    The time - in seconds - for the prediction window to end on, respective of the
    current date. The length of the prediction window being the max - min times.<br>
    e.g 1209600 (would have a prediction window of 14 days)

*   Stop On First Positive Label *Required*<br>
    Stop considering a user once they have a positive label.<br>
    e.g true

*   Lookback Gap In Seconds *Required*<br>
    Gap - in seconds - to add between the current date and the lookback window,
    typically this is 1 day to simulate a real life scoring scenario.<br>
    e.g 86400 (for 1 day)

*   (Lookback) Window Time In Seconds *Required*<br>
    The size - in seconds - of the lookback window.<br>
    e.g 7776000 (for 90 days)

*   Output Sliding Window AVRO Prefix *Required*<br>
    The location on Google Cloud Storage to output the AVRO files to, for windowed
    data at date intervals based on the sliding amount specified.<br>
    e.g gs://[GCS BUCKET]/windowing-output/

*   Output Session Based Window AVRO Prefix *Required*<br>
    The location on Google Cloud Storage to output the AVRO files to, for windowed
    data at date intervals based on Google Analytics sessions. In this data one
    window is output for every session, with the session being the last in the
    window.<br>
    e.g gs://[GCS BUCKET]/windowing-session-output/

#### To run this directly (using the examples from options above):

    $ mvn -Pdataflow-runner compile exec:java@run \
        -Dexec.mainClass=com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.SlidingWindowPipeline \
        -Dexec.args="--runner=DataflowRunner \
        --project='[CLOUD PROJECT TO RUN IN]' \
        --inputAvroSessionsLocation='gs://[GCS BUCKET]/usersession-output/Nov26_01/*.avro' \
        --snapshotStartDate='01/01/2017' \
        --snapshotEndDate='01/08/2017' \
        --slideTimeInSeconds='604800' \
        --minimumLookaheadTimeInSeconds='86400' \
        --maximumLookaheadTimeInSeconds='1209600' \
        --stopOnFirstPositiveLabel='true' \
        --lookbackGapInSeconds='86400' \
        --windowTimeInSeconds='7776000' \
        --outputSlidingWindowAvroPrefix='gs://[GCS BUCKET]/windowing-output/' \
        --outputSessionBasedWindowAvroPrefix='gs://[GCS BUCKET]/windowing-session-output/'"

Once built, open Dataflow in your Google Cloud Platform project to see the
pipeline running.

#### To build a template:

    $ mvn -Pdataflow-runner compile exec:java \
        -D_MAIN_CLASS=SlidingWindowPipeline \
        -D_PROJECT_ID=[CLOUD PROJECT TO RUN IN] \
        -D_OUTPUT_LOCATION=gs://[GCS BUCKET]/windowing-template \
        -D_TEMP_LOCATION=gs://[GCS BUCKET]/temp/mlwp_templates


SessionBasedWindowPipeline: This way to run this pipeline is almost identical
to the SlidingWindowPipeline. The main difference is that there is no slide_time
parameter.


### Step 4. GenerateFeaturesPipeline.java

#### Options:

*   Windowed AVRO Location *Required*<br>
    The location of the Sliding or Session Window AVRO files from Step 3.<br>
    e.g gs://[GCS BUCKET]/windowing-output/Nov26_01/*.avro

*   Feature Destination Table *Required*<br>
    The location of the BigQuery Features table (Note: Ensure the pipeline has
    write access to this).<br>
    e.g myproject:mydataset.ga_features_table
    **Note: the format[project_id]:[dataset_id].[table_id]**

*   Training Mode<br>
    Set false to exclude predictionLabel from the pipeline output. Default value is true.

*   Show Effective Date<br>
    Set false to exclude effectiveDate from the pipeline output. Default value is true.

*   Show Start Time<br>
    Set false to exclude startTime from the pipeline output. Default value is true.

*   Show End Time<br>
    Set false to exclude startTime from the pipeline output. Default value is true.

*   Show Effective Date Week Of Year<br>
    Set false to exclude effectiveDateWeekOfYear from the pipeline output. Default value is true.

*   Show Effective Date Month Of Year<br>
    Set false to exclude effectiveDateMonthOfYear from the pipeline output. Default value is true.

#### Feature Options:

These options aggregate activity within the lookback window to create features.

To define these options the input expects the format:

    columnName:[list of values to consider]:[default value]

Where *columnName* represents the column name from the BigQuery dataset (we used
in Step 1), and [...] a list of possible values for that column, to generate the
categorical variables, numerical variables...etc (Note: for some of these
features that list maybe empty). For custom dimensions, *columnName* is
*customDimension.index.X* where X is the custom dimension index number.

If required a comma separated list of these inputs can be used with the format:

    columnName1:[list of values to consider]:[default value],columnName2:[list of values to consider]:[default value],columnName3:[list of values to consider]:[default value]

Note: if you do not need to generate a certain type of feature for this dataset
then the option does not need to be used.

**For Numerical Variables:**

*   Sum Value From Variables<br>
    This feature calculates the total sum of all numeric values of a given
    variable within the lookback window. The input expects the column name
    containing values to sum.<br>
    e.g totals.hits:[]:[]

*   Average By Tenure Value From Variables<br>
    This feature calculates the average per day of all numeric values of a given
    variable within the lookback window (or their tenure if it's smaller than the
    lookback window size). The input expects just a column name that represents an
    active user, with an empty list.<br>
    e.g totals.hits:[]:[]

*   Average Value From Variables<br>
    This feature calculates the average for values in the lookback window. The
    input expects  just a column name that contains the values to average, with an
    empty list.<br>
    e.g totals.hits:[]:[]

**For Categorical Variables:**

*   Count Value From Variables<br>
    This feature counts the number of occurrences of a value in the lookback
    window. The input expects a column name and a list of values to count, with a
    default if the value not within the list of values submitted.<br>
    e.g channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,
    Affiliates,Others]:[Others]

*   Most Freq Value From Variables<br>
    This feature calculates the most frequent value active within the lookback
    window. The input expects a column name and list of values to find the most
    frequent from, with a default if the value not within the list of values
    submitted.<br>
    e.g  channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,
    Affiliates,Others]:[Others]

*   Proportions Value From Variables<br>
    This feature calculates the proportions a set of values are active within the
    lookback window. The input expects a column name and list of values to find
    proportions from, with a default if the value not within the list of values
    submitted.<br>
    e.g  channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,
    Affiliates,Others]:[Others]

*   Recent Value From Variables<br>
    This feature extracts the most recent value used in the lookback window. The
    input expects a list of values to consider for a column, and a default if the
    value not within the list of values submitted.<br>
    e.g channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,
    Affiliates,Others]:[Others]

#### To run this directly (using the examples from options above):

    $ mvn -Pdataflow-runner compile exec:java@run \
        -Dexec.mainClass=com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.GenerateFeaturesPipeline \
        -Dexec.args="--runner=DataflowRunner \
        --project='[CLOUD PROJECT TO RUN IN]' \
        --windowedAvroLocation='gs://[GCS BUCKET]/windowing-output/Nov26_01/*.avro' \
        --featureDestinationTable='myproject:mydataset.ga_features_table' \
        --sumValueFromVariables='totals.hits:[]:[]' \
        --proportionsValueFromVariables='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates,Others]:[Others]' \"

Once built, open Dataflow in your Google Cloud Platform project to see the
pipeline running.

#### To build a template:

    $ mvn -Pdataflow-runner compile exec:java \
        -D_MAIN_CLASS=GenerateFeaturesPipeline \
        -D_PROJECT_ID=[CLOUD PROJECT TO RUN IN] \
        -D_OUTPUT_LOCATION=gs://[GCS BUCKET]/feature-template \
        -D_TEMP_LOCATION=gs://[GCS BUCKET]/temp/mlwp_templates

## Reference

* [Running Cloud Dataflow Templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#custom-templates)
