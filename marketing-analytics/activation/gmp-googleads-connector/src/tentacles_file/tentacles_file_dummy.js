// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Tentacles File empty implementation.
 */

'use strict';

const {TentaclesFile} = require('./tentacles_file.js');

/**
 * Tentacles File implementation with no extra data source, only prints logs.
 * @implements {TentaclesFile}
 */
class TentaclesFileDummy {

  /** @override */
  save(file) {
    console.log('[Dummy File saver] save: ', file);
    return Promise.resolve(file.name);
  }

  /** @override */
  saveError(fileId, error) {
    console.log(`[Dummy File saver] ${fileId} has error: `, error);
    return Promise.resolve(fileId);
  }
}

module.exports = {TentaclesFileDummy};
