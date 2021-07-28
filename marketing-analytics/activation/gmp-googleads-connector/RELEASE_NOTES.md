Release Notes
===


1.2.5 (2021-07-27)
---

### Enhancements

* Support data files that contain non-ASCII characters
* Add a configuration item in Google Analytics Data Import to delete previous
  uploaded files.

1.2.0 (2021-06-01)
---

### Integrated APIs

* Added Google Ads Customer Match user upload through Google Ads API
* Added Measurement Protocol integration with GA4

1.0.0 (2021-02-23)
---

### Bug fix

* [Unable to Install on GCP](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/23)

0.1.2 (2020-12-18)
---

**Note: If you are upgrading Tentacles to a pre-existent Tentacles instance, run
`./deploy.sh` in Cloud Shell to re-install Tentacles and after that run
`./deploy.sh update_api_config` to update the configuration of APIs.**

Features added and bugs fixed in v0.1.2.

### Cloud Functions Runtime

* Nodejs10 is now the default Cloud Functions runtime.
  [issue](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/15)

### Integrated APIs

* Added Search Ads 360 conversions insert
* Added Google Ads offline conversions upload through Google Ads API

### Authentication methods

* Added OAuth. Google Ads API only supports OAuth, so if Google Ads API is
  selected, OAuth will be used to authenticate. Otherwise, users can select to
  use OAuth or service account key file to authenticate.

### Other updates

* Added `{}` as the marker of `API` and `config` in the file names to solve this
  [issue](https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues/14).
