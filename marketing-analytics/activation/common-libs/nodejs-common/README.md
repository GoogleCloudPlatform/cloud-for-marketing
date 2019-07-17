# NodeJS Common Library

<!--* freshness: { owner: 'lushu' reviewed: '2019-06-27' } *-->

Node Js common library for other projects, e.g. Tentacles. The library includes:

1.  Authentication wrapper based on google auth library to support OAuth, JWT
    and ADC authentication;

2.  Wrapper for some Google API client libraries, mainly used in 'Tentacles':

    *   Google Analytics Data Import

    *   Google Analytics Measurement Protocol

    *   Campaign Manager Offline Conversion Upload

3.  Utilities wrapper class for Google Cloud Products:

    *   **Cloud Components Factory Function**: Different Google Cloud Components
        have different initialization behavior of their client libraries. This
        function offers a unified way to get an instance of a given Cloud
        components and other initial parameters.

    *   **Cloud Functions Adapter**: Cloud Functions have different parameters
        in different environments, e.g. Node6 vs Node8[[1]]. This utility file
        offers an adapter to wrap a Node8 Cloud Functions into Node6 and Node8
        compatible functions.

    *   **Firestore Access Object**: Firestore has two modes which are excluded
        to each other and can't be changed once selected in a Cloud
        Project[[2]]. This class, with its two successors offer a unified
        interface to operate data objects on either Firestore or Datastore.

    *   **Pub/Sub Utilities**: Offers utilities functions to create topics and
        subscriptions for Pub/Sub, as well as the convenient way to publish a
        messages.

    *   **Storage Utilities**: Offers functions to manipulate the files on Cloud
        Storage. The main functions are:

        *   Reading a given length (or slightly less) content without breaking a
            line;
        *   Splitting a file into multiple files with the given length (or
            slightly less) without breaking a line;
        *   Merging files into one file.

[1]:https://cloud.google.com/functions/docs/writing/background#functions-writing-background-hello-pubsub-node8-10
[2]:https://cloud.google.com/datastore/docs/concepts/overview#comparison_with_traditional_databases
