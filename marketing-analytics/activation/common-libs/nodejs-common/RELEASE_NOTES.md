# Release Notes

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
