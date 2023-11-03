# NodeJS Common Library

<!--* freshness: { owner: 'lushu' reviewed: '2022-04-07' } *-->

A NodeJs common library for other projects, e.g. [GMP and Google Ads Connector]
and [Data Tasks Coordinator]. This library includes:

1. Authentication wrapper based on google auth library to support OAuth, JWT and
   ADC authentication;

1. Wrapper for some Google APIs for integration, mainly
   for [GMP and Google Ads Connector]:

   - Google Analytics data import
   - Google Analytics measurement protocol
   - Campaign Manager offline conversion upload
   - Search Ads 360 conversions upload
   - Google Ads click conversions upload
   - Google Ads customer match upload
   - Google Ads enhanced conversions upload
   - Google Ads conversions scheduled uploads based on Google Sheets
   - Measurement Protocol Google Analytics 4

1. Wrapper for some Google APIs for reporting, mainly
   for [Data Tasks Coordinator]:

   - Google Ads reporting
   - Campaign Manager reporting
   - Search Ads 360 reporting
   - Display and Video 360 reporting
   - YouTube Data API
   - Ads Data Hub querying

1. Utilities wrapper class for Google Cloud Products:

   - **Firestore Access Object**: Firestore has two modes[[comparison]] which
     have different API. This class, with its two successors, offer a unified
     interface to operate data objects within or not a transaction on either
     Firestore Native mode or Firestore Datastore mode.

   - **AutoMl Tables API**: Offers a unified entry to use this API based on
     Google Cloud client library combined with REST requests to service
     directly due to some functionalities missed in the client library.

   - **Vertex AI API**: Offers a unified entry to use this API based on Google
     Cloud client library.

   - **Pub/Sub Utilities**: Offers utilities functions to create topics and
     subscriptions for Pub/Sub, as well as the convenient way to publish a
     message.

   - **Storage Utilities**: Offers functions to manipulate the files on Cloud
     Storage. The main functions are:

     - Reading a given length (or slightly less) content without breaking a
       line;
     - Splitting a file into multiple files with the given length (or
       slightly less) without breaking a line;
     - Merging files into one file.

   - **Cloud Scheduler Adapter**: A wrapper to pause and resume Cloud Scheduler
     jobs.

   - ~~**Cloud Functions Adapter**: Cloud Functions have different parameters in
     different environments, e.g. Node6 vs Node8. This utility file offers
     an adapter to wrap a Node8 Cloud Functions into Node6 and Node8 compatible
     functions.~~ (This has been removed since v1.9.0)

1. A share library for [Bash] to facilitate installation tasks.

[gmp and google ads connector]: https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/activation/gmp-googleads-connector
[data tasks coordinator]: https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/activation/data-tasks-coordinator
[comparison]: https://cloud.google.com/datastore/docs/firestore-or-datastore
[bash]: https://www.gnu.org/software/bash/
