# NodeJS Common Library

<!--* freshness: { owner: 'lushu' reviewed: '2021-12-02' } *-->

A NodeJs common library for other projects, e.g. [GMP and Google Ads Connector]
and [Data Tasks Coordinator]. This library includes:

1. Authentication wrapper based on google auth library to support OAuth, JWT and
   ADC authentication;

1. Wrapper for some Google APIs for integration, mainly
   for [GMP and Google Ads Connector]:

    * Google Analytics data import
    * Google Analytics measurement protocol
    * Campaign Manager offline conversion upload
    * Search Ads 360 conversions upload
    * Google Ads click conversions upload
    * Google Ads customer match upload
    * Google Ads conversions scheduled uploads based on Google Sheets
    * Measurement Protocol Google Analytics 4

1. Wrapper for some Google APIs for reporting, mainly
   for [Data Tasks Coordinator]:

    * Google Ads reporting
    * Campaign Manager reporting
    * Search Ads 360 reporting
    * Display and Video 360 reporting
    * YouTube Data API
    * Ads Data Hub querying

1. Utilities wrapper class for Google Cloud Products:

    * **Firestore Access Object**: Firestore has two modes which are excluded to
      each other and can't be changed once selected in a Cloud Project[[2]].
      This class, with its two successors offer a unified interface to operate
      data objects on either Firestore or Datastore.

    * **AutoMl Tables API**: Offers a unified entry to use this API based on
      Google Cloud client library combined with REST requests to service
      directly due to some functionalities missed in the client library.

    * **Vertex AI API**: Offers a unified entry to use this API based on Google
      Cloud client library.

    * **Pub/Sub Utilities**: Offers utilities functions to create topics and
      subscriptions for Pub/Sub, as well as the convenient way to publish a
      message.

    * **Storage Utilities**: Offers functions to manipulate the files on Cloud
      Storage. The main functions are:

        * Reading a given length (or slightly less) content without breaking a
          line;
        * Splitting a file into multiple files with the given length (or
          slightly less) without breaking a line;
        * Merging files into one file.

    * **Cloud Scheduler Adapter**: A wrapper to pause and resume Cloud Scheduler
      jobs.

    * **Cloud Functions Adapter**: Cloud Functions have different parameters in
      different environments, e.g. Node6 vs Node8[[1]]. This utility file offers
      an adapter to wrap a Node8 Cloud Functions into Node6 and Node8 compatible
      functions.

1. A share library for [Bash] to facilitate installation tasks.

[GMP and Google Ads Connector]:https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/activation/gmp-googleads-connector

[Data Tasks Coordinator]:https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/activation/data-tasks-coordinator

[1]:https://cloud.google.com/functions/docs/writing/background#functions-writing-background-hello-pubsub-node8-10

[2]:https://cloud.google.com/datastore/docs/concepts/overview#comparison_with_traditional_databases

[Bash]:https://www.gnu.org/software/bash/
