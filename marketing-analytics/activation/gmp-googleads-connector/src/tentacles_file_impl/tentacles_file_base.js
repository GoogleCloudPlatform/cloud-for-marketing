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
 * @fileoverview Tentacles File base class.
 */

'use strict';

/**
 * Tentacles File implementation base class. Without external data source, it
 * just takes logs.
 */
class TentaclesFileBase {
  /**
   * Saves the file Entity based on the given event information.
   * @param {{
   *   name:string,
   *   bucket:string,
   *   size:number,
   *   updated:!Date,
   * }} file File data comes event.
   * @return {!Promise<string|number>} Saved File document/entity ID.
   */
  save(file) {
    console.log('[Dummy File saver] save: ', file);
    return Promise.resolve(file.name);
  }

  /**
   * Saves the error message of this file.
   * @param {string|number} fileId File ID.
   * @param {string} error Error message for a file.
   * @return {!Promise<string|number>}
   */
  saveError(fileId, error) {
    console.log(`[Dummy File saver] ${fileId} has error: `, error);
    return Promise.resolve(fileId);
  }
}

exports.TentaclesFileBase = TentaclesFileBase;
