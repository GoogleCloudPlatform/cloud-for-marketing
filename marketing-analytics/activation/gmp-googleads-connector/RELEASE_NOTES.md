# Release Notes

## 4.0.1 (2023-06-27)

### Integrated APIs

- Updated CM360 Conversion API with a new field
[UserIdentifier](https://developers.google.com/doubleclick-advertisers/rest/v4/Conversion#useridentifier).

### Change the Cloud Functions deployment to Artifact Registry

## 4.0.0 (2023-06-15)

### New Features

- Auto-sizing: when there is no `size` configuration in the file name, Tentacles
  will automatically decide a size based on QPS and average length of lines.
- Auto-retry: when unexpected errors or timeout happen, Cloud Functions
  **apirequester** will auto retry upto 3 times.
- Speed up: for some connectors, e.g. `MP_GA4`, `MP`, `ACLC`, `ACA` and `ACM`,
  multiple instances of Cloud Functions **apirequester** are availabe now.
  The sending rate can be increased 5 to 10 times.


### Integrated APIs

- Updated to the v13 of the Google Ads API.

### Bug fix

- Fixed: Some Google Ads related errors can not be show properly in the dashboard.
- Fixed: If there was a [BOM](https://en.wikipedia.org/wiki/Byte_order_mark) at
 the beginning of data files, the first line is not a valid JSON string and the
 process will fail. Now, Tentacles will ignore the BOM.

## 3.1.0 (2023-03-08)

### New Features

- Google Ads conversion adjustment upload service (code `ACA`) supports
`RESTATEMENT` conversion adjustment
- Google Ads conversion upload service (code `ACLC`) supports `gbraid` and
`wbraid` as click identifier
- Google Ads offline user data job service (code `AOUD`) suports new types of job:
  - `STORE_SALES_UPLOAD_FIRST_PARTY` with [`TransactionAttribute`](https://developers.google.com/google-ads/api/reference/rpc/latest/TransactionAttribute)
  - `CUSTOMER_MATCH_WITH_ATTRIBUTES` with [`UserAttribute`](https://developers.google.com/google-ads/api/reference/rpc/latest/UserAttribute)

### Integrated APIs

- Updated to the v12 of the Google Ads API.

### Bug fix

- Fixed: task was terminated immediately when Measurement Protocol for GA4 got a 502 error from the server.
- Fixed: workflow was broken when there were too many failed records which could not be saved in to Firestore `datastore` mode.

## 3.0.0 (2022-11-10)

### New Features

- [Secret Manager](https://cloud.google.com/secret-manager) is now used as the
  preferred way to store and offer OAuth token (the content of the generated
  `oauth.token.json` file). Each API configuration can have its own
  authorization token now by setting a property `secretName` in the
  configuration JSON.
  - The explicit service account key file is going to be deprecated due to the concerns of security. If you want to use service account authorization for some APIs, use the default service account of Cloud Functions.
- A new Google Sheets based tool will replace current `deploy.sh` to install
  Tentacles. This tool also offers following functions:
  - Update installed Tentacles to a new version
  - Enable new API connectors
  - Edit API configurations and upload them to Firestore
  - Test Tentacles by uploading a temporary file with the given test data
  - Create OAuth token and save it to Secret Manager
  - List all available secret names in Secret Manager

### Integrated APIs

- Updated to the v4 of the Campaign Manager 360 API.

### Bug fix

- Fixed the problem of spammed logs from the Cloud Functions `tentacles_tran`
  which is caused by the [failure of `message.ack()`](
    https://github.com/googleapis/nodejs-pubsub/issues/1648).

## 2.4.0 (2022-10-14)

### New Features

- User list name is supported by Google Ads Customer Match upload (ACM)
  connector. If the list doesn't exist, it will be created automatically.
  Requires new configuration items. For details,
  see README
- Fixed an issue related to 'Channel credentials must be a ChannelCredentials
  object' error

## 2.3.0 (2022-08-10)

### Integrated APIs

- Updated to the v11 of the Google Ads API.
- Google Ads API for Call Conversions Upload
- Google Ads API for OfflineUserDataService Upload

## 2.2.0 (2022-06-14)

### New Installation command

- To activate other APIs that were not selected during the installation, run
  `./deploy.sh reselect_api` to reselect the APIs.

## 2.1.0 (2022-04-08)

### Integrated APIs

- Updated to the v10 of the Google Ads API.

## 2.0.5 (2022-03-30)

### Integrated APIs

- Google Ads API for Enhanced Conversions Upload

## 2.0.4 (2022-03-08)

### Updated OAuth flow

- With the [deprecation of oob flow](https://developers.googleblog.com/2022/02/making-oauth-flows-safer.html#disallowed-oob), Tentacles OAuth flow now uses loopback address to replace oob.
- Usually, users don't have a web server to host on the lookback address and this will make the browser to land on a 'non existing' page.
- This looks like 'something went wrong' but it is actually expected behavior.
- Users should follow the instructions and copy/paste the whole URL to continue.

## 2.0.3 (2022-02-23)

### Integrated APIs

- Updated Google Ads Customer Match connector
  for [this change](https://ads-developers.googleblog.com/2021/10/userdata-enforcement-in-google-ads-api.html)

Note: For ACM connector, the new solution is much slower than previous version
due to the reduced data in each request. DO
use [size parameter](https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/main/marketing-analytics/activation/gmp-googleads-connector#34-name-convention-of-data-files)
in the file name to guide Tentacles to properly slice your data file into small
pieces rather than relying on the default setting. Ideally each piece should
contain about 5k lines of data, which should be done in about 5 minutes. For
example, if 5k lines are about 0.4 MB, then put this `_size{0.4}` in your data
filenames.

## 2.0.0 (2021-12-02)

### Visualization

- A Data Studio dashboard is available to show the process and results of
  Tentacles.

### Authorization

- Cloud Functions supports the extension of authorization scopes now:
  - In the case of authorization based on service account, no explicit service
    account nor the key file will be created by the installation script. The
    default service account of Cloud Functions will be used.
  - If there is an existing service account key in local, Tentacles will
    continue deploying it and using it. There is no breaking behavior.
  - If you want to remove the extra service account and the key file, you can
    delete the key file and re-deploy Tentacles. In this way, you may need to
    grant access to the new service account (subject to the target systems).
    You can use the command `./deploy.sh print_service_account` to print out
    the service account that will be used.

### Cloud Functions Runtime

- Nodejs14 is now the default Cloud Functions runtime.

### Integrated APIs

- Updated to the v9 of the Google Ads API.

### Enhancements

- Measurement Protocol for GA4 connector support nested objects merge.
- Google Ads Conversions uploading supports customer variables.
- Measurement Protocol, Measurement Protocol for GA4 and Google Ads Conversions
  uploading now support 'debug' mode.

---

## 1.4.0 (2021-09-07)

### Integrated APIs

- Updated to the v8 of the Google Ads API

## 1.2.5 (2021-07-27)

### Enhancements

- Support data files that contain non-ASCII characters
- Add a configuration item in Google Analytics Data Import to delete previous
  uploaded files.

## 1.2.0 (2021-06-01)

### Integrated APIs

- Added Google Ads Customer Match user upload through Google Ads API
- Added Measurement Protocol integration with GA4

## 1.0.0 (2021-02-23)

### Bug fix

- [Unable to Install on GCP](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/23)

---

## 0.1.2 (2020-12-18)

**Note: If you are upgrading Tentacles to a pre-existent Tentacles instance, run
`./deploy.sh` in Cloud Shell to re-install Tentacles and after that run
`./deploy.sh update_api_config` to update the configuration of APIs.**

Features added and bugs fixed in v0.1.2.

### Cloud Functions Runtime

- Nodejs10 is now the default Cloud Functions runtime.
  [issue](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/15)

### Integrated APIs

- Added Search Ads 360 conversions insert
- Added Google Ads offline conversions upload through Google Ads API

### Authorization methods

- Added OAuth. Google Ads API only supports OAuth, so if Google Ads API is
  selected, OAuth will be used to authenticate. Otherwise, users can select to
  use OAuth or service account key file to authenticate.

### Other updates

- Added `{}` as the marker of `API` and `config` in the file names to solve this [issue](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/14).
