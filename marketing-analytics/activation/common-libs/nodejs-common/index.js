// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Nodejs common library export file.
 */

'use strict';

exports.api = require('./apis/index.js');
exports.base = require('./base/base_dao.js');
exports.CloudFunctionsUtils = require('./components/cloudfunctions_utils.js');
exports.FirestoreAccessBase = require('./components/firestore_access_base.js');
exports.DatastoreModeAccess = require('./components/datastore_mode_access.js');
exports.NativeModeAccess = require('./components/native_mode_access.js');
exports.PubSubUtils = require('./components/pubsub_utils.js');
exports.StorageUtils = require('./components/storage_utils.js');
exports.utils = require('./components/utils.js');

exports.getCloudProduct = require('./components/gcloud.js');
exports.BigQueryStorageConnector = require('./components/bigquery_storage.js');
