# Release Notes

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
