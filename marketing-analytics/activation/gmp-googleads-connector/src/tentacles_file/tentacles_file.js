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
 * @fileoverview Interface for operations of Tentacles File.
 */

'use strict';

/**
 * @typedef {{
 *   name:string,
 *   bucket:string,
 *   fileSize:string,
 *   updated:string,
 *   error:(string|undefined),
 *   attributes:{
 *     api:(undefined|string),
 *     config:(undefined|string),
 *     dryRun:(undefined|string),
 *     gcs:(undefined|string),
 *     size:(undefined|string),
 *     topic:(undefined|string),
 *   }
 * }}
 */
let TentaclesFileEntity;

/**
 * Tentacles File interface to log incoming files.
 * @interface
 */
class TentaclesFile {
  /**
   * Saves the file Entity based on the given event information.
   * @param {!TentaclesFileEntity} file File data comes event.
   * @return {!Promise<string|number>} Saved File document/entity ID.
   */
  save(file) {}

  /**
   * Saves the error message of this file.
   * @param {string|number} fileId File ID.
   * @param {string} error Error message for a file.
   * @return {!Promise<string|number>}
   */
  saveError(fileId, error) {}
}

module.exports = {
  TentaclesFileEntity,
  TentaclesFile,
};
