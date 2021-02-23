# GMP and Google Ads Connector

<!--* freshness: { owner: 'lushu' reviewed: '2019-08-27' } *-->

Disclaimer: This is not an official Google product.

GMP and Google Ads Connector (code name **Tentacles**) is an out-of-box solution
based on Google Cloud Platform. It can send a massive amount data to GMP (e.g.
Google Analytics, Campaign Manager) or Google Ads in a automatic and reliable
way.

## Contents

- [1. Key Concepts](#1-key-concepts)
  - [1.1. Challenges for sending data to APIs](#11-challenges-for-sending-data-to-apis)
  - [1.2. Solution architecture overview](#12-solution-architecture-overview)
  - [1.3. Summary](#13-summary)
- [2. Installation](#2-installation)
  - [2.1. Create/use a Google Cloud Project with a billing account](#21-createuse-a-google-cloud-projectgcp-with-a-billing-account)
  - [2.2. Check the permissions](#22-check-the-permissions)
  - [2.3. Check out source codes](#23-check-out-source-codes)
  - [2.4. Run install script](#24-run-install-script)
- [3. Post Installation](#3-post-installation)
  - [3.1. Create Firestore database if there is no one](#31-create-firestore-database-if-there-is-no-one)
  - [3.2. Accounts in external systems](#32-accounts-in-external-systems)
  - [3.3. Configurations of APIs](#33-configurations-of-apis)
  - [3.4. Name convention of data files](#34-name-convention-of-data-files)
  - [3.5. Content of data files](#35-content-of-data-files)
- [4. API Details](#4-api-details)
  - [4.1. MP: Google Analytics Measurement Protocol](#41-mp-google-analytics-measurement-protocol)
  - [4.2. GA: Google Analytics Data Import](#42-ga-google-analytics-data-import)
  - [4.3. CM: DCM/DFA Reporting and Trafficking API to upload offline conversions](#43-cm-dcmdfa-reporting-and-trafficking-api-to-upload-offline-conversions)
  - [4.4. SFTP: Business Data upload to Search Ads 360](#44-sftp-business-data-upload-to-search-ads-360)
  - [4.5. GS: Google Ads conversions scheduled uploads based on Google Sheets](#45-gs-google-ads-conversions-scheduled-uploads-based-on-google-sheets)
  - [4.6. SA: Search Ads 360 conversions insert](#46-sa-search-ads-360-conversions-insert)
  - [4.7. ACLC: Google Ads Click Conversions upload](#47-aclc-google-ads-click-conversions-upload-via-api)
  - [4.8. ACM: Google Ads Customer Match upload](#48-acm-google-ads-customer-match-upload-via-api)

## 1. Key Concepts

### 1.1. Challenges for sending data to APIs

1.  Different APIs have different idioms, authentication and QPS.
2.  Sending out big data is a long process with some APIs' QPS limitation.
3.  Maintaining the reliability of long process requires brings extra
    complexity.

### 1.2. Solution architecture overview

```
 ▉▉▉        trigger         ▒ ▒ ▒ ▒ ▒ ▒
═════> (gcs) ──────> [init] ═══════════════> {ps-data}
 File                  |    Slice & publish      ║
                       |    data messages        ║ ▒
               Publish |                         ║     Pull one message
              'nudge'  └---> {ps-tran} ──────> [tran]  from the queue and
               message          ^      trigger   ║     pass it to the next
                                │                ║ ▒
LEGEND                          │                V        ▒               ▒
(..): Cloud Storage             │             {ps-api} ══════> [api] ▸▹▸▹▸▹▸▹▸▹ GMP/Google Ads
[..]: Cloud Functions           │                      trigger   |   Send out
{..}: Pub/Sub Topic             └--------------------------------┘   requests
▉/▒ : Data/Data piece                 Publish 'nudge' message
==> : Data flow
──> : Event flow
- ->: Publish 'nudge' message
```

**Components' description:**

Components       | GCP Product     | Description
---------------- | --------------- | ---------------------------------------
(gcs)            | Cloud Storage   | New files will automatically trigger Cloud Functions **\[initiator\]** to start the whole process.
\[init\]iator    | Cloud Functions | Triggered by **(gcs)**, it validates the file first. Then it slices and publishes the data as messages to **{ps-data}**. After that, it sends a **nudge** message to **{ps-tran}**.
{ps-data}        | Pub/Sub         | This message queue will hold the data from incoming files. It only has pull subscriptions. So all data will be held here until taken away intendedly.
{ps-tran}        | Pub/Sub         | This topic triggers Cloud Functions **\[transporter\]** which manages the pull subscription of **{ps-data}**.
\[tran\]sporter  | Cloud Functions | Triggered by new messages to **{ps-tran}**, it will pull one message from **{ps-data}** and publish to **{ps-api}**.
{ps-api}         | Pub/Sub         | Trigger source for the Cloud Functions **\[apirequester\]**. It will pass the data from **\[transporter\]** to **\[apirequester\]**.
\[api\]requester | Cloud Functions | Triggered by **{ps-api}**, it sends the data to the corresponding API endpoint with proper configuration based on the incoming file name.

### 1.3. Summary

1.  Cloud Functions **initiator**, **transporter** and Pub/Sub topics
    **ps-data**, **ps-tran**, **ps-api** altogether compose a serverless system
    that can hold big data while offer pieces of data in an on-demand way. This
    system is unrelated to APIs[1].

2.  Cloud Functions **apirequester** is a single purposed function to send out a
    small piece of data to the given target (backend of the API). It maintains
    the feasibility to extend for new APIs.

[1]: Actually, every API has its own **ps-data** topic. But all these topics
    are created during installation.

## 2. Installation

### 2.1. Create/use a Google Cloud Project(GCP) with a billing account

1.  How to [Creating and Managing Projects][create_gcp]
2.  How to [Create, Modify, or Close Your Billing Account][billing_gcp]

[create_gcp]:https://cloud.google.com/resource-manager/docs/creating-managing-projects
[billing_gcp]:https://cloud.google.com/billing/docs/how-to/manage-billing-account

### 2.2. Check the permissions

Parts of Cloud Project permissions are required to install this solution.
Usually, a user as the role 'Project Editor' can do the installation. If more
precise permissions are preferred, the user may need following roles:

*   Storage Admin
*   Pub/Sub Editor
*   Cloud Functions Developer
*   Cloud Datastore User
*   Service Account User (to activate the default service account for Cloud
    Functions)
*   Service Usage Admin (to activate APIs)

If the APIs integrated need a service account to do the authentication, then
following roles are required, too.

*   Service Account Admin
*   Service Account Key Admin

During the installation, the script will check whether does current user have enough permissions to continue. It would be more smooth if you can check the permissions before start installation.

**Tip: Single role 'Project Editor' is enough to carry out an installation.**

### 2.3. Check out source codes

1.  Open the [Cloud Shell](https://cloud.google.com/shell/)
2.  Clone the repository:

```shell
git clone https://github.com/GoogleCloudPlatform/cloud-for-marketing.git
```

### 2.4. Run install script

Run the installation script and follow the instructions:

```shell
cd cloud-for-marketing/marketing-analytics/activation/gmp-googleads-connector; bash deploy.sh
```

During the installation process, the script will do following:

1.  Enable Google Cloud Components and Google APIs
    *   [Cloud Functions](https://console.cloud.google.com/functions)
    *   [Pub/Sub](https://console.cloud.google.com/cloudpubsub)
    *   [Storage](https://console.cloud.google.com/storage)
    *   [Firestore](https://console.cloud.google.com/firestore)
2.  Create or use an existent Storage bucket for coming file
3.  Create Topics and Subscriptions of Pub/Sub
4.  Create a service account and download the key file (if required)
5.  Deploy Cloud Functions of Tentacles

## 3. Post Installation

### 3.1. Create Firestore database if there is no one

This solution will use Firestore to save the API configuration, logs, etc. In
new Cloud Projects, users need to manually create a Firestore database before
use it. Follow the link given at the end of installation and create the database
if necessary.

Note: Whenever the location or mode of Firestore is confirmed, neither could be
changed again in this project. So be cautious when make decision of this.

### 3.2. Accounts in external systems

API integration is not a single side effort. Besides installing Tentacles, the
user needs to get the API server side and the data both ready.

Note: this chapter will cover these topics in a general way. Please refer every
API's chapter for the details.

Besides the **Google Analytics Measurement Protocol** or **SFTP** uploading,
most of the APIs need its own 'accounts' to manage the access. For example, we
need to have an 'account' in Google Analytics before we can use Data Import to
upload file into Google Analytics.

In that case, the user needs to use the email of the service account which is
created during the installation process to create those 'accounts' in the
specific systems and grant them with proper access.

Tip: If you forgot the email of the service account, check `client_email` in the
file `keys/service-account.key.json`.

### 3.3. Configurations of APIs

Different APIs have different configurations, as well as one API could have more
than one configurations for different usages, e.g. Google Analytics Data Import
can have multiple uploaded datasets. Hence, configurations are grouped by APIs
and combine a single JSON object in the file `config_api.json`.

Following is a sample config that contains a configuration item named `foo` for
Google Analytics Data Import and another one named `bar` for Measurement
Protocol.

```json
{
  "GA": {
    "foo": {
      "dataImportHeader": "[YOUR-DATA-IMPORT-HEADER]",
      "gaConfig": {
        "accountId": "[YOUR-GA-ACCOUNT-ID]",
        "webPropertyId": "[YOUR-WEB-PROPERTY-ID]",
        "customDataSourceId": "[YOUR-CUSTOM-DATASOURCE-ID]"
      }
    }
  },
  "MP": {
    "bar": {
      "mpConfig":{
        "v": "1",
        "t": "transaction",
        "ni": "1",
        "dl": "[YOUR-SOMETHING-URL]",
        "tid": "[YOUR-WEB-PROPERTY-ID]"
      }
    }
  }
}
```

After editing the content in `config_api.json`, run following script to
synchronize the configuration to Google Cloud Firestore/Datastore. Tentacles
will pick up the latest configuration automatically.

```shell
bash deploy.sh update_api_config
```

### 3.4. Name convention of data files

After deployment, Tentacles will monitor all the files in the Storage bucket.
Tentacles isn't designed for a fixed API or configurations. On the contrary, it
is easy to extend for new APIs or configurations. To achieve that, Tentacles
expects the incoming data filenames contain the pattern **API{X}** and
**config{Y}**.

*   `X` stands for the API code, currently available values:

    *   **MP**: Google Analytics Measurement Protocol
    *   **GA**: Google Analytics Data Import
    *   **CM**: DCM/DFA Reporting and Trafficking API to upload offline
        conversions
    *   **SFTP**: SFTP upload
    *   **ACLC**: Google Ads Click Conversion Upload via API

*   `Y` stands for the config name, e.g. `foo` or `bar` in the previous case.

Other optional patterns in filename:

*   If the filename contains `dryrun`, then the whole process will be carried
    out besides that the data won't be really sent to API serve.

*   If the filename contains `_size{Z}` or `_size{Zmb}`, the incoming file will
    be divided into multiple files with the size under `Z`MB in Cloud Storage.
    For some APIs, Tentacles will use files in Cloud Storage to transfer the
    data instead of the Pub/Sub. (The reason is for those 'file uploading' type
    of APIs, using file directly is more efficient.) In that case, this setting
    will make sure the files won't oversize the target systems' capacity.

**Note: Previous square brackets as the file name marker (e.g. API[X]) is also
supported. But it is not convenient to copy those files through gsutil.**

### 3.5. Content of data files

For most APIs data, Tentacles expects the data format as JSONL(newline-delimited
JSON). Every line is a valid JSON string. This is one of the
[BigQuery export format](https://cloud.google.com/bigquery/docs/exporting-data#export_formats_and_compression_types).

There are two exceptions:

1.  **SFTP**: This API will upload the given file to the server through SFTP no
    matter what the format of files is.
2.  **GA**: Google Analytics Data Import only support CSV files.

## 4. API Details

### 4.1. MP: Google Analytics Measurement Protocol

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | MP
Data Format             | JSONL
Authentication          | No need
What Tentacles does     | Merging the **data** with the **configuration** to build the hit URLs and sent them to Google Analytics.
**Usage Scenarios**     | Uploading offline conversions
Transfer Data on        | Pub/Sub
Require Service Account | NO
Request Type            | HTTP Post
\# Records per request  | 20
QPS                     | -
Reference               | [Measurement Protocol Overview][mp_doc]

[mp_doc]: https://developers.google.com/analytics/devguides/collection/protocol/v1/

*   *Sample configuration piece:*

```json
{
  "qps": 10,
  "numberOfThreads": 10,
  "mpConfig":{
    "v": "1",
    "t": "transaction",
    "ni": "1",
    "dl": "[YOUR-SOMETHING-URL]",
    "tid": "[YOUR-WEB-PROPERTY-ID]"
  }
}
```

Note: Optional `qps` and `numberOfThreads` are used to control the sending
speed. `qps` is the 'queries per second' and stands for how many requests will
be sent simultaneously.

Tip: All these properties names in configuration `mpConfig` or data file can be
find in
[Measurement Protocol Parameter Reference](https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters).

*   *Sample Data file content:*

```json
{"cid":"1471305204.1541378885","ti":"123","tr":"100"}
```

### 4.2. GA: Google Analytics Data Import

API Specification       | Value
----------------------- | ------------------------------------------------
API Code                | GA
Data Format             | CSV
Authentication          | [Create an account based][create_ga_account] for the Service Account's email with the [Edit permission][grant_ga_permission]
What Tentacles does     | Using the GA data import configuration to upload the file directly.
**Usage Scenarios**     | Upload user segment information from prediction results of Machine Learning models or CRM
Transfer Data on        | GCS
Require Service Account | YES
Request Type            | File upload based on GA API Client Library
\# Records per requests | - / The maximum of single file size is 1GB
QPS                     | -
Reference               | [Custom Data import example][ga_example]

[create_ga_account]:https://support.google.com/analytics/answer/1009694?hl=en
[grant_ga_permission]:https://support.google.com/analytics/answer/2884495
[ga_example]:https://support.google.com/analytics/answer/3191417?hl=en

*   *Sample configuration piece:*

```json
{
  "dataImportHeader": "[YOUR-DATA-IMPORT-HEADER]",
  "gaConfig": {
    "accountId": "[YOUR-GA-ACCOUNT-ID]",
    "webPropertyId": "[YOUR-WEB-PROPERTY-ID]",
    "customDataSourceId": "[YOUR-CUSTOM-DATASOURCE-ID]"
  }
}
```

Note: `dataImportHeader` is the header of uploading CSV file. It is fixed in
Google Analytics when the user set up the Data Import item and it must exist in
the file uploaded. \
Sometimes, the data files do not have such a header line, e.g. BigQuery cannot
export data with semicolon in column names. In that case, an explicit config
item `dataImportHeader` is required, just like this sample. Tentacles will
automatically attach this line at the beginning of data file before uploading.

*   *Sample Data file content:*

```
395040310.1546495163,NO
391813229.1541728717,YES
1277688775.1563499908,YES
1471305204.1541378885,NO
```

### 4.3. CM: DCM/DFA Reporting and Trafficking API to upload offline conversions

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | CM
Data Format             | JSONL
Authentication          | [Create a User Profile][create_user_profile] for the Service Account's email with a role that has the **Insert offline conversion** permission
What Tentacles does     | Combined the **data** with the **configuration** to build the request body and sent them to CM API endpoint.
**Usage Scenarios**     | 1. Personalizing Ads via user variables in conversions;<br> 2. Uploading verified(offline process) purchases as the **real** transactions to let CM do the optimization based on better data.
Transfer Data on        | Pub/Sub
Require Service Account | YES
Request Type            | HTTP Request to RESTful endpoint
\# Records per request  | 1,000
QPS                     | 1
Reference               | [Overview: DCM/DFA Reporting and Trafficking API's Conversions service][conversions_overview]

[create_user_profile]:https://support.google.com/dcm/answer/6098287?hl=en
[conversions_overview]:https://developers.google.com/doubleclick-advertisers/guides/conversions_overview

*   *Sample configuration piece:*

```json
{
  "cmAccountId": "[YOUR-DCM-ACCOUNT-ID]",
  "cmConfig": {
    "idType": "encryptedUserId",
    "conversion": {
      "floodlightConfigurationId": "[YOUR-FL-CONFIG-ID]",
      "floodlightActivityId": "[YOUR-FL-ACTIVITY-ID]",
      "quantity": 1
    },
    "customVariables": [
      "U2"
    ],
    "encryptionInfo": {
      "encryptionEntityId": "[YOUR-ENCRYPTION-ID]",
      "encryptionEntityType": "DCM_ADVERTISER",
      "encryptionSource": "AD_SERVING"
    }
  }
}
```

*   Fields' definition:
    *   `idType`, value can be: `encryptedUserId`, `gclid` or `mobileDeviceId`.
        *   `encryptedUserId`, a single encrypted user ID obtained from the %m
            match macro or Data Transfer.
        *   `gclid`, a Google Click Identifier generated by Google Ads or Search
            Ads 360.
        *   `mobileDeviceId`, an unencrypted mobile ID in the IDFA or AdID
            format.
    *   `conversion`, common properties for a conversion, e.g. floodlight info.
    *   `customVariables`, an array of user variable names. It can be omitted if
        not required.
    *   `encryptionInfo`, only required as the `idType` is `encryptedUserId`

Note: Campaign Manager offline conversions can be attributed to a click, a
device, or a user ID. For each request, there should be one and only one
attribution type. That's defined by `idType`.

Tip: For more details of a request, see
[Upload conversions](https://developers.google.com/doubleclick-advertisers/guides/conversions_upload)

*   *Sample Data file content:*

Attributed to a Google Click Identifier (`gclid`):

```json
{"gclid":"EAIaIQobChMI3_fTu6O4xxxPwEgEAAYASAAEgK5VPD_example","timestampMicros":"1550407680000000"}
```

Attributed to encrypted user ID (`encryptedUserId`):

```json
{"encryptedUserId":"EAIaIQobChMI3_fTu6O4xxxPwEgEAAYASAAEgK5VPD_example","U2":"a|b|c"}
```

### 4.4. SFTP: Business Data upload to Search Ads 360

API Specification       | Value
----------------------- | -----------------------------------------------------
API Code                | SFTP
Data Format             | any
Authentication          | Specific user/password, unrelated to Service Account
What Tentacles does     | Connect the SFTP server and upload the file
**Usage Scenarios**     | Uploading Business Data feed file for Search Ads 360.
Transfer Data on        | GCS
Require Service Account | NO
Request Type            | SFTP file transfer
\# Records per request  | -
QPS                     | -
Reference               | -

Note: Though SFTP is a general purpose file transfer protocol, Tentacles
supports this mainly for Search Ads 360 Business Data upload. For more
information, see
[Introduction to business data in Search Ads 360](https://support.google.com/searchads/answer/6342986?hl=en&ref_topic=6343015).

*   *Sample configuration piece:*

```json
{
  "fileName": "[FILENAME_YOU_WANTED_TO_SFTP]",
  "sftp": {
    "host": "[YOUR-SFTP-HOST]",
    "port": "[YOUR-SFTP-PORT]",
    "username": "[YOUR-SFTP-USERNAME]",
    "password": "[YOUR-SFTP-PASSWORD]"
  }
}
```

Tip: Optional `fileName` is used to give the uploaded file a fixed name. It
supports `TIMESTAMP` as a placeholder for a uploading timestamp string value. \
If `fileName` is omitted, a default file name based on the ingested one will be
generated.

*   *Sample Data file content:*

It could be any file.


### 4.5. GS: Google Ads conversions scheduled uploads based on Google Sheets

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | GS
Data Format             | CSV
Authentication          | Add the Service Account's email as the editor of the Google Spreadsheet that Google Ads will consume.
What Tentacles does     | Load the **data** from the CSV to the given SpreadSheet through Google Sheets API.
**Usage Scenarios**     | Import conversions from ad clicks into Google Ads for non Google Ads API clients.
Transfer Data on        | GCS
Require Service Account | YES
Request Type            | HTTP Request to RESTful endpoint
\# Records per request  | -
QPS                     | -
Reference               | [Import conversions from ad clicks into Google Ads][import_conversions]<br>[Google Sheets API][sheets_api]

[import_conversions]:https://support.google.com/google-ads/answer/7014069
[sheets_api]:https://developers.google.com/sheets/api/reference/rest/

Google Sheets API can be used to read, write, and format data in Sheets. Google Ads supports to use 
a Google Sheet as the data source of scheduled uploads of conversions. The extra benefit is this 
integration does **not** require an Google Ads Developer Token.

Note: Google Sheets has the limit of 10,000,000 cells for a single SpreadSheet. If every conversion 
needs 5 properties/columns/fields, then for a single Spreadsheet data source, there could be about 2
millions conversions supported.

*   *Sample configuration piece:*

```json
{
  "spreadsheetId": "[YOUR-SPREADSHEET-ID]",
  "sheetName": "[YOUR-SHEET-NAME]",
  "sheetHeader": "[ANYTHING-PUT-AHEAD-OF-CSV]",
  "pasteData": {
    "coordinate": {
      "rowIndex": 0,
      "columnIndex": 0
    },
    "delimiter": ","
  }
}
```

*   Fields' definition:
    *   `spreadsheetId`, the [Spreadsheet ID](https://developers.google.com/sheets/api/guides/concepts#spreadsheet_id).
    *   `sheetName`, the name of sheet that will load CSV data.
    *   `sheetHeader`, fixed row(s) at the top of the Sheet before the CSV data. This is optional.
    *   `pasteData`, [`PasteDataRequest`][PasteDataRequest] for Sheets API batchUpdate.
    
[PasteDataRequest]:https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#pastedatarequest

*   *Sample Data file content:*

```
Google_Click_ID,Conversion_Name,Conversion_Time,Conversion_Value,Conversion_Currency
EAIaIQobChMIrLGCr66x4wIVVeh3Ch2eygjsEAAYASAAEgIFAKE_BwE,foo,2019-07-13 00:02:00,1,USD
EAIaIQobChMIvdTVjayx4wIV0eF3Ch0eGQAQEAAYBCAAEgIFAKE_BwE,foo,2019-07-13 00:04:00,1,USD
EAIaIQobChMI8rrrq62x4wIVQbDtCh2D3QU0EAAYAiAAEgIFAKE_BwE,foo,2019-07-13 00:01:00,1,USD
```

### 4.6. SA: Search Ads 360 conversions insert

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | SA
Data Format             | JSONL
Authentication          | TODO: [Create a User Profile][create_user_profile] for the Service Account's email with a role that has the **Insert offline conversion** permission
What Tentacles does     | Combined the **data** with the **configuration** to build the request body and sent them to SA360 API endpoint.<br> If the data file is empty, then a [updateAvailability][update_availability] request is sent out instead.
**Usage Scenarios**     | Import conversions from ad clicks into Search Ads.
Transfer Data on        | Pub/Sub
Require Service Account | YES
Request Type            | HTTP Request to RESTful endpoint
\# Records per request  | 200
QPS                     | 10
Reference               | [Search Ads 360 API > Conversion: insert][insert_conversions]

[import_conversions]:https://support.google.com/google-ads/answer/7014069
[insert_conversions]:https://developers.google.com/search-ads/v2/reference/conversion/insert
[update_availability]:https://developers.google.com/search-ads/v2/reference/conversion/updateAvailability

*   *Sample configuration piece:*

```json
{
  "saConfig": {
    "type": "TRANSACTION",
    "segmentationType": "FLOODLIGHT",
    "segmentationId": "[YOUR-SEGMENTATION-ID]",
    "state": "ACTIVE"
  },
  "availabilities": [
    {
      "agencyId": "[YOUR-AGENCY-ID]",
      "advertiserId": "[YOUR-ADVERTISER-ID]",
      "segmentationType": "FLOODLIGHT",
      "segmentationId": "[YOUR-SEGMENTATION-ID]"
    }
  ]
}
```

*   Fields' definition:
    *   `saConfig`, the configuration for conversions
        *   `type`, .
        *   `segmentationType`, 
        *   `segmentationId`,.
        *   `state`, "ACTIVE".
    *   `availabilities`, the configuration for updating availabilities.
        *   `agencyId`, agency Id.
        *   `advertiserId`, advertiser Id.
        *   `segmentationType`, 
        *   `segmentationId`,
        
[PasteDataRequest]:https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#pastedatarequest

*   *Sample Data file content:*

```
{"clickId":"Cj0KCQjwiILsBRCGARIsAHK","conversionTimestamp":"1568766602000","revenueMicros":"1571066"}
```

### 4.7. ACLC: Google Ads Click Conversions upload via API

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | ACLC
Data Format             | JSONL
Authentication          | OAuth
What Tentacles does     | Combined the **data** with the **configuration** to build the request body and sent them to Ads API endpoint via [3rd party API Client library][google_ads_api].
**Usage Scenarios**     | Uploading offline sales and other valuable actions to target and optimize your campaigns for increased profit based on better data.
Transfer Data on        | Pub/Sub
Require Service Account | NO
Request Type            | HTTP Request to RESTful endpoint
\# Records per request  | 2,000
QPS                     | 1
Reference               | [Overview: Offline Conversion Imports][offline_conversions_overview]

[google_ads_api]:https://opteo.com/dev/google-ads-api#upload-conversionclick
[offline_conversions_overview]:https://developers.google.com/google-ads/api/docs/conversions/overview#offline_conversions

*   *Sample configuration piece:*

```json
{
  "customerId": "[YOUR-GOOGLE-ADS-ACCOUNT-ID]",
  "loginCustomerId": "[YOUR-LOGIN-GOOGLE-ADS-ACCOUNT-ID]",
  "developerToken": "[YOUR-GOOGLE-ADS-DEV-TOKEN]",
  "adsConfig": {
    "conversion_action": "[YOUR-CONVERSION-ACTION-NAME]",
    "conversion_value": "[YOUR-CONVERSION-VALUE]",
    "currency_code": "[YOUR-CURRENCY-CODE]"
  }
}
```

*   Fields' definition:
    *   `customerId`, Google Ads Customer account Id 
    *   `loginCustomerId`, Login customer account Id (MCC Account Id)
    *   `developerToken`, Developer token to access the API.
    *   `conversion_action`, Resource name of the conversion action in the format `customers/${customerId}/conversionActions/${conversionActionId}`
    *   `conversion_value`, The default value of the conversion for the advertiser
    *   `currency_code`, The default currency associated with conversion value; ISO 4217 3 character currency code e.g. EUR, USD

Tip: For more details see
[Upload click conversions](https://developers.google.com/google-ads/api/reference/rpc/v4/ClickConversion)

*   *Sample Data file content:*

Attributed to a Google Click Identifier (`gclid`):

```json
{"conversion_date_time":"2020-01-01 03:00:00-18:00", "conversion_value":"20", "gclid":"EAIaIQobChMI3_fTu6O4xxxPwEgEAAYASAAEgK5VPD_example"}
```

### 4.8. ACM: Google Ads Customer Match upload via API

API Specification       | Value
----------------------- | --------------------------------------------------
API Code                | ACM
Data Format             | JSONL
Authentication          | OAuth
What Tentacles does     | Combined the **data** with the **configuration** to build the request body and sent them to Ads API endpoint via [3rd party API Client library][google_ads_node].
**Usage Scenarios**     | Customer Match lets you use your online and offline data to reach and re-engage with your customers across Search, Shopping, Gmail, YouTube, and Display.
Transfer Data on        | Pub/Sub
Require Service Account | NO
Request Type            | HTTP Request to RESTful endpoint
\# Records per request  | 1000
QPS                     | 1
Reference               | [Overview: Customer Match][customer_match_overview]

[google_ads_node]:https://github.com/Opteo/google-ads-node
[customer_match_overview]:https://developers.google.com/google-ads/api/docs/remarketing/audience-types/customer-match

*   *Sample configuration piece:*

```json
{
  "developerToken": "[YOUR-GOOGLE-ADS-DEV-TOKEN]",
  "customerMatchConfig": {
    "customer_id": "[YOUR-GOOGLE-ADS-ACCOUNT-ID]",
    "login_customer_id": "[YOUR-LOGIN-GOOGLE-ADS-ACCOUNT-ID]",
    "list_id": "[YOUR-CUSTOMER-MATCH-LIST-ID]",
    "list_type": "[YOUR-CUSTOMER-MATCH-LIST-TYPE]",
    "operation": "create|remove"
  }
}
```

*   Fields' definition:
    *   `developerToken`, Developer token to access the API.
    *   `customer_id`, Google Ads Customer account Id 
    *   `login_customer_id`, Login customer account Id (MCC Account Id)
    *   `list_id`, User List id for customer match audience
    *   `list_type`, Must be one of the following: hashed_email, hashed_phone_number, mobile_id, third_party_user_id or address_info; [Read more about id types][user_identifier]
    *   `operation`, Can be either create or remove in single file; [Read more about operation][user_data_operation]

[user_identifier]:https://developers.google.com/google-ads/api/reference/rpc/v4/UserIdentifier
[user_data_operation]:https://developers.google.com/google-ads/api/reference/rpc/v4/UserDataOperation

Tip: For more details see
[USer Data Service](https://developers.google.com/google-ads/api/reference/rpc/v4/UserDataService)

*   *Sample Data file content:*

```json
{"hashed_email": "47b2a4193b6d05eac87387df282cfbb326ec5296ba56ce8518650ce4113d2700"}
```