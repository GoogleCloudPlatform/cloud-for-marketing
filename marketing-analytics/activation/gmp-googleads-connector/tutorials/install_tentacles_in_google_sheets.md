# Install Tentacles in a Google Sheets based tool<!-- omit from toc -->

<!--* freshness: { owner: 'lushu' reviewed: '2023-03-09' } *-->

Disclaimer: This is not an official Google product.

![Demo](./images/cyborg/Cyborg_start_installation.gif)

Tentacles has a Google Sheets based installation tool (Code name **Cyborg**).
This tool was designed to replace previous Bash script
[`deploy.sh`](../README.md#24-run-install-script) with many enhanced
features, including:

1. Show what happened when Tentacles was installed and keep a record of all
   kinds of GCP resources that were enabled, checked, created or updated.
1. Upgrade to a new version or install a designated version of Tentacles.
1. Enable new connectors after Tentacles was installed.
1. Edit and upload the API configuration in this Google Sheets.
1. Generate and manage OAuth tokens which will be saved in the GCP
   [`Secret Manager`](https://cloud.google.com/secret-manager).
1. Different credentials (OAuth tokens) are supported for different integration
   configurations.
1. Test API configuration for accessibility in the Sheets.
1. Test installed Tentacles by sending a piece of data in this Google Sheets to
   the target Cloud Storage bucket.

Other differences should be noted are:

1. It can not deploy your customised Tentacles. If you added a new connector or
   made some modification in your forked repository, you need to use the previous
   Bash script to install it.
1. It can not deploy OAuth token files or service account key files in your
   local file system:
   - For OAuth token files, you can save them in
     [`Secret Manager`](https://cloud.google.com/secret-manager) or use the new
     tool to create a new token;
   - For service account key files, they have been deprecated due to security
     concerns. If you prefer using service accounts as the identities for target
     systems, you can use the service account of Cloud Functions (the Sheets
     will show the service account).

## Contents<!-- omit from toc -->

- [1. Preparation](#1-preparation)
  - [1.1. Create/use a Google Cloud project with a billing account](#11-createuse-a-google-cloud-project-with-a-billing-account)
  - [1.2. Create/check the OAuth consent screen](#12-createcheck-the-oauth-consent-screen)
  - [1.3. Make a copy of this tool](#13-make-a-copy-of-this-tool)
  - [1.4. Update Google Cloud project number for the Apps Script](#14-update-google-cloud-project-number-for-the-apps-script)
  - [1.5. (Optional) Create an OAuth 2.0 client ID](#15-optional-create-an-oauth-20-client-id)
  - [1.6. (Optional) Create an Explicit Authorization](#16-optional-create-an-explicit-authorization)
- [2. Understand the tool](#2-understand-the-tool)
  - [2.1. Sheets](#21-sheets)
  - [2.2. Menu](#22-menu)
- [3. Installation](#3-installation)
  - [3.1. Sheet `Tentacles`](#31-sheet-tentacles)
  - [3.2. Menu `🤖 Cyborg` -\> `Tentacles`](#32-menu--cyborg---tentacles)
    - [3.2.1. Submenu item `Check resources`](#321-submenu-item-check-resources)
    - [3.2.2. Submenu item `Apply changes`](#322-submenu-item-apply-changes)
    - [3.2.3. Submenu item `Recheck resources (even it is OK)`](#323-submenu-item-recheck-resources-even-it-is-ok)
    - [3.2.4. Submenu item `Reset sheet`](#324-submenu-item-reset-sheet)
- [4. Configuration](#4-configuration)
  - [4.1. (Optional) Generate an OAuth token](#41-optional-generate-an-oauth-token)
    - [4.1.1. OAuth sidebar](#411-oauth-sidebar)
    - [4.1.2. Sheet `Secret Manager`](#412-sheet-secret-manager)
  - [4.2. Create an integration configuration](#42-create-an-integration-configuration)
  - [4.3. Menu `🤖 Cyborg` -\> `Tentacles Config`](#43-menu--cyborg---tentacles-config)
    - [4.3.1. Submenu item `Check selected config for accessibility`](#431-submenu-item-check-selected-config-for-accessibility)
    - [4.3.2. Submenu item `Upload selected config to Firestore`](#432-submenu-item-upload-selected-config-to-firestore)
    - [4.3.3. Submenu item `Upload selected data to test Tentacles`](#433-submenu-item-upload-selected-data-to-test-tentacles)
    - [4.3.4. Submenu item `Upload all configs to Firestore`](#434-submenu-item-upload-all-configs-to-firestore)
    - [4.3.5. Submenu item `Reset sheet (will lose modification)`](#435-submenu-item-reset-sheet-will-lose-modification)

## 1. Preparation

### 1.1. Create/use a Google Cloud project with a billing account

1.  How to [Create and Manage Projects][create_gcp]
1.  How to [Create, Modify, or Close Your Billing Account][billing_gcp]

[create_gcp]: https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project
[billing_gcp]: https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account

### 1.2. Create/check the OAuth consent screen

If there is no OAuth consent screen in this Google Cloud project, you need to
[configure the OAuth consent screen][oauth_consent] first. When you create the
consent screen, some settings need to be:

1. `Publishing status` as `In production`, otherwise the refresh token will
   expire every 7 days.
1. `User type` could be `External` or check [here][user_type] for more details.
1. No scopes need to be entered.

[oauth_consent]: https://developers.google.com/workspace/guides/configure-oauth-consent
[user_type]: https://support.google.com/cloud/answer/10311615?hl=en#zippy=%2Cexternal%2Cinternal

### 1.3. Make a copy of this tool

1.  Join the [Tentacles External Users][group] group and wait for approval.
1.  After you join the group, you can visit the [Google Sheets tool][cyborg]
    and make a copy.

[group]: https://groups.google.com/g/tentacles-external-users
[cyborg]: https://docs.google.com/spreadsheets/d/1T4gWMKZv5TyqwInCuu1bo2leswTycIn72Wjbvvqf6WM/copy

### 1.4. Update Google Cloud project number for the Apps Script

1. Get your GCP project number. See how to
   [determine the project number of a standard Cloud project][project_number]
1. Open Apps Script editor window by clicking Google Sheets menu
   `Extensions` -> `Apps Script`
1. On the Apps Script window, click `⚙️` (Project Settings) at the left menu bar,
   then click the button `Change project`.
1. Enter the project number and click the button `Set project`.

[project_number]: https://developers.google.com/apps-script/guides/cloud-platform-projects#determine_the_id_number_of_a_standard

> **NOTE:** If you could not update the project number and got an error
> saying `You cannot switch to a Cloud Platform project outside this script owner's Cloud Organization`, you can set up an [explicit authorization](#16-optional-create-an-explicit-authorzation) for your
> copy of this tool.

### 1.5. (Optional) Create an OAuth 2.0 client ID

Some APIs (e.g. Google Ads API) require OAuth for authorization. To use OAuth,
you need to prepare an OAuth 2.0 client ID.

1. How to [Create an OAuth 2.0 client ID][create_oauth_client]. Select the
   `Application type` as [`Desktop`][oauth_client_desktop].

[create_oauth_client]: https://developers.google.com/workspace/guides/create-credentials#oauth-client-id
[oauth_client_desktop]: https://support.google.com/cloud/answer/6158849?hl=en#zippy=%2Cnative-applications%2Cdesktop-apps

### 1.6. (Optional) Create an Explicit Authorization

> **NOTE:** If possible, use [1.4. Update Google Cloud project number for the Apps Script
> ](#14-update-google-cloud-project-number-for-the-apps-script) to complete the
> setting for you copy of the tool. This approach is a workaround.

1. Prepare an OAuth Client by following [1.5. (Optional) Create an OAuth 2.0
   client ID](#15-optional-create-an-oauth-20-client-id). A different OAuth
   client to your data integration is suggested.
2. Click menu `🤖 Cyborg` -> `Explicit Authorization`. This will open a
   sidebar titled `Explicit Authorization`.
3. In the `Create new explicit authorization` section, enter the
   `OAuth client ID` and `client secret` that you created previously.
4. Click the `Start` button and complete the OAuth confirmation process in the
   newly opened tab and land on an error page _"This site can't be reached"_.
   **This is an expected behaviour.**
5. Copy the `url` of the error page and paste it back to the sidebar.
6. `Cyborg` would complete the OAuth process and put a refresh token in the
   `textarea` named `Generated OAuth token`.
7. Click the button `Save as explicit authorization` and wait for it to complete.

## 2. Understand the tool

This Google Sheets based tool has four sheets and a top level menu:

### 2.1. Sheets

1. Sheet `README`: information sheet
1. Sheet `Tentacles`: main sheet to install or manage Tentacles
1. Sheet `Tentacles Config`: the configuration of integration. This is used to
   replace the JSON file `config_api.json` in the previous version.
1. Sheet `Secret Manager`: list of names of saved secrets.

### 2.2. Menu

The top level menu `🤖 Cyborg` contains following items:

1. `Tentacles`: submenu to carry out installation tasks
1. `Tentacles Config`: submenu to check, upload and test
   [API configurations](../README.md#32-configurations-of-apis)
1. `Secret Manager`: submenu to list secrets
1. `Generate an OAuth token`: open a sidebar to help users generate an OAuth
   token and save it to `Secret Manager`. The saved secret(s) can be used in API
   configuration to replace previous file-based token or key files.
1. `Explicit Authorization`: open a sidebar to check, create or delete the
   explicit authorization of the current sheet. See [1.6. (Optional) Create an
   Explicit Authorzation](#16-optional-create-an-explicit-authorzation)

## 3. Installation

How to install Tentacles: (1) switch to sheet `Tentacles` and input
required information in the sheet; (2) use menu `Check resources` to run
a check. If an error happened, fix it and retry `Check resources`; (3) after
all checks passed, use menu `Apply changes` to complete the installation.

### 3.1. Sheet `Tentacles`

This sheet contains a list of Cloud resources that will be operated during
installation. You do not need to edit most of them except:

1. Yellow background fields that need user input or confirm, e.g. `Project Id`.
2. Tick checkboxes to select `Connectors` that you are going to use.
3. Unselect `Tentacles Dashboard` if you do not want to use it.

### 3.2. Menu `🤖 Cyborg` -> `Tentacles`

When you click items under `🤖 Cyborg` for the first time, a dialog window
titled `Authorization Required` will pop up to ask for confirmation.
After you complete it, just click the menu item again as you originally clicked.

#### 3.2.1. Submenu item `Check resources`

This item does the most jobs with no or minor changes to the GCP project. If
anything wrong happened, it would pause and mark the related resource's `status`
as `ERROR` and the reason would be appended.

Based on the GCP's situation, it might pause several times especially when a
new GCP project is involved, including:

1. Ask users to select the `Location` for Cloud Functions and Cloud Storage
1. Ask users to select the mode and location to create a `Firestore` instance

You can always re-run `Check resources` after you fix the problems or after you
make any changes, e.g. select another version of Tentacles. All passed resources
have the `status` as `OK`

#### 3.2.2. Submenu item `Apply changes`

After all resources have passed the check, some resources have the `status` as
`TO_APPLY`. These resources are usually major changes and need
users to confirm before the process, e.g. deploying Cloud Functions, creating a
new Cloud Storage bucket, creating a new BigQueery dataset, etc.

Click menu `Apply changes` to apply those changes.

> **NOTE:** For non Google Workspace accounts, the script runtime is up to 6 min
> / execution. So possibly an `exceeded max execution time` occurs when you are
> deploying all three Cloud Functions. If that happens, just click `Apply changes`
> again.

#### 3.2.3. Submenu item `Recheck resources (even it is OK)`

For efficiency, `Cyborg` skips most of all `OK` resources when it carries out
the task `Check resources`. However in some circumstances, if you would like to
have a forced `Check` everything, you can use this item.

#### 3.2.4. Submenu item `Reset sheet`

If your sheet `Tentacles` was messed up and you want a clean start, use this
menu.

> **NOTE:** Only all information in this sheet would be wiped out, the GCP
> resources are untouched.

## 4. Configuration

After Tentacles is installed, switch to sheet `Tentacles Config` to manage the
configuration.

If your integration requires an OAuth token, we can create one first.

### 4.1. (Optional) Generate an OAuth token

#### 4.1.1. OAuth sidebar

1. Click menu `🤖 Cyborg` -> `Generate an OAuth token`. This will open a
   sidebar titled `OAuth2 token generator`.
1. Enter the `OAuth client ID` and `client secret` that you created previously.
1. Select the API scopes that you want to grant access.
1. Click the `Start` button and complete the OAuth confirmation process in the
   newly opened tab and land on an error page _"This site can't be reached"_.
   **This is an expected behaviour.**
1. Copy the `url` of the error page and paste it back to the `OAuth` sidebar.
1. `Cyborg` would complete the OAuth process and put a refresh token in the
   `textarea` named `Generated OAuth token`.
1. Enter a name for this token to be saved in `Secret Manager`.
1. Click the button `Save` and wait for it to complete.

Now you can use the saved token through the secret name in API configuration.

#### 4.1.2. Sheet `Secret Manager`

After you saved the OAuth token in the sidebar, you can list it in the sheet
`Secret Manager` by click menu `🤖 Cyborg` -> `Secret Manager`
-> `Refresh secrets`.

### 4.2. Create an integration configuration

There are configurations with placeholders for most all connectors. You can make
a copy of the (line) API that you want to use. For your copied configuration:

1. Do not change column `API`
1. Change column `Config` to your preferred name
1. Edit column `Config Content`. This content in this column will be
   automatically turned into a readable JSON format. If the content was not a valid
   JSON string, it will turn to red colour.

> **NOTE:** When you set up a data pipeline to generate data files for
> Tentacles, you need to make sure the file name follows the previous [name
> convention](../README.md#33-name-convention-of-data-files).

### 4.3. Menu `🤖 Cyborg` -> `Tentacles Config`

After you complete your API configuration, you can use the menu to verify,
upload or test it.

#### 4.3.1. Submenu item `Check selected config for accessibility`

This item is used to do a basic access check for those APIs that need
authentication.

Select a cell or whole line of the configuration that you want to check, click
the menu. `Cyborg` will use the `secret` based on the `secretName` in the
configuration to do an accessibility check. The result will be shown in the
column `API Access Check`

#### 4.3.2. Submenu item `Upload selected config to Firestore`

Select a cell or whole line of the configuration and click this item, the
selected configuration will be saved to Firestore.

#### 4.3.3. Submenu item `Upload selected data to test Tentacles`

Input some sample data in the column `Test Data` and click this item, `Cyborg`
will send the content as a file to the target `Cloud Storage` bucket, within the
`Monitored Folder`. The file name will follow the name convention to trigger
`Tentacles` task.

For the sample data format, see [`Content of data files`](../README.md#34-content-of-data-files)

#### 4.3.4. Submenu item `Upload all configs to Firestore`

Upload all API configurations to Firestore. It is used to replace the previous
bash command `update_api_config`.

#### 4.3.5. Submenu item `Reset sheet (will lose modification)`

Clean this sheet and generate a new one with those template configurations only.
