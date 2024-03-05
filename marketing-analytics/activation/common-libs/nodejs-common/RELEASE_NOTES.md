# Release Notes

## 2.1.0 (2024-03-05)

### Update to the v3 of the DV360 API
### Update to the v16 of the Google Ads API

## 2.0.0 (2023-11-24)

### Firestore's non-default database is supported
### Ads Data Hub supports parameter definition
### Download Campaign Manager reports as streams

## 1.9.0 (2023-10-12)

### Update to the v14 of the Google Ads API
### Support SendGrid API
### Upgrade the default Cloud Functions runtime to Nodejs18
### Remove support for Cloud Function runtime Nodejs6

## 1.8.0 (2023-09-26)

### Enhance support for Ads Data Hub API

## 1.7.1 (2023-06-27)

### Change the Cloud Functions deployment to Artifact Registry
### Enhance CM360 Conversion API

## 1.7.0 (2023-06-15)

### Update to the v13 of the Google Ads API

## 1.6.0 (2023-03-08)

### Update to the v12 of the Google Ads API
### Update to the v2 of the DV360 API
### Add new feature: Store sales data upload through OfflineUserDataJobService

## 1.5.0 (2022-11-10)

### New feature: AuthClients supports Secret Managers as the credentials provider
### Update to the v4 of the Campaign Manager 360 API
### Add a method to acknowledge Pub/Sub messages with resolved Promise

## 1.4.0 (2022-10-10)

### Add new feature: Query or create Google Ads Customer Match User Lists
### Update dependent libraries to the latest version to solve an issue related to 'Channel credentials must be a ChannelCredentials object' error

## 1.3.0 (2022-08-10)

### Update to the v11 of the Google Ads API
### Add new feature: Google Ads call conversions upload
### Add new feature: support Google Ads OfflineUserDataService

## 1.1.0 (2022-04-07)

### Update to the v10 of the Google Ads API

## 1.0.5 (2022-03-30)

### Add new feature: Google Ads enhanced conversions upload

## 1.0.4 (2022-03-08)

### Updated OAuth flow

With the [deprecation of oob flow](https://developers.googleblog.com/2022/02/making-oauth-flows-safer.html#disallowed-oob), Tentacles OAuth flow now uses loopback address to replace oob.
Usually, users don't have a web server to host on the lookback address and this will make the browser to land on a 'non existing' page. This looks like 'something went wrong' but it is actually expected behavior.
Users should follow the instructions and copy/paste the whole URL to continue.

## 1.0.3 (2022-02-23)

### Update Customer Match on Google Ads based on [this change](https://ads-developers.googleblog.com/2021/10/userdata-enforcement-in-google-ads-api.html)

## 1.0.1 (2022-01-05)

### Support creating a Firestore database during installation

## 1.0.0 (2021-12-02)

### Change default behavior of service account authorization

Remove steps to create a service account and download service account key file.
Use the default service account of Cloud Functions.

### Upgrade Cloud Functions runtime to Nodejs14

Dependencies libraries update to latest stable version.

### Update to the v9 of the Google Ads API

### Connect to YouTube Data API

## 0.9.1 (2021-09-08)

### Add Vertex AI API for batch prediction.

## 0.9.0 (2021-09-07)

### Update to the v8 of the Google Ads API

## 0.8.3 (2021-08-29)

### Add a bash function to get OAuth access token from the OAuth key file.

## 0.8.1 (2021-08-12)

### Refactor 'location' confirmation when creating dataset or bucket.

## 0.7.7 (2021-08-06)

### Support timezone input/confirmation.

## 0.7.6 (2021-07-24)

### Google Analytic Data Import supports to clear previously uploaded data files.

## 0.7.2 (2021-06-09)

### Support Non-ASCII data files

## 0.7.0 (2021-03-03)

### Upgrade Google Ads API to v7.0

## 0.5.1 (2021-04-07)

### Add Api for Measurement Protocol Google Analytics 4

## 0.5.0 (2021-03-03)

### Add component of Google Cloud AutoMl Tables API
