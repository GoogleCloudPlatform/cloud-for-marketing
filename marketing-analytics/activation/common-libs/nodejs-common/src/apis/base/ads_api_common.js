// Copyright 2024 Google Inc.
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
 * @fileoverview Common functions for Google Ads and Search Ads API classes.
 */

'use strict';

const { Transform } = require('stream');
const {
  getFilterAndStringifyFn,
  getLogger,
} = require('../../components/utils.js');

/**
 * Returns a integer format CID by removing dashes and spaces.
 * @param {string} cid
 * @return {string}
 */
function getCleanCid(cid) {
  return cid.toString().trim().replace(/-/g, '');
}

const START_TAG = '"results":';
const FIELD_MASK_TAG = '"fieldMask"';
const END_TAG = '"requestId"';

/**
 * A stream.Transform that can extract properties and convert naming of the
 * response of Google/Search Ads report from REST interface.
 */
class RestSearchStreamTransform extends Transform {

  /**
   * @constructor
   * @param {boolean=} snakeCase Whether or not output JSON in snake naming.
   * @param {function|undefined} postProcessFn An optional function to process
   *     the data after the default process. The default process includes
   *     filtering out fields based on `fieldMask` and adjusting the naming
   *     convention.
   */
  constructor(snakeCase = false, postProcessFn) {
    super({ objectMode: true });
    this.snakeCase = snakeCase;
    this.chunks = [Buffer.from('')];
    this.processFn; // The function to process a row of the report.
    this.logger = getLogger('ADS.STREAM.T');
    this.stopwatch = Date.now();
    this.postProcessFn = postProcessFn;
  }

  _transform(chunk, encoding, callback) {
    const latest = Buffer.concat([this.chunks[this.chunks.length - 1], chunk]);
    const endIndex = latest.indexOf(END_TAG);
    if (endIndex > -1) {
      this.chunks.push(chunk);
      const rawString = Buffer.concat(this.chunks).toString();
      const maskIndex = rawString.lastIndexOf(FIELD_MASK_TAG);
      if (!this.processFn) {
        const fieldMask = rawString
          .substring(maskIndex + FIELD_MASK_TAG.length, rawString.indexOf(END_TAG))
          .split('"')[1];
        this.logger.debug(`Got fieldMask: ${fieldMask}`);
        const processFn = getFilterAndStringifyFn(fieldMask, this.snakeCase);
        if (this.postProcessFn) {
          this.processFn = (obj) => {
            return this.postProcessFn(processFn(obj));
          }
        } else {
          this.processFn = processFn;
        }
      }
      let rows, data;
      if (rawString.indexOf(START_TAG) === -1) { // no 'results'
        rows = [];
        data = '';
      } else {
        const startIndex = rawString.indexOf(START_TAG) + START_TAG.length;
        const resultsWithTailing = rawString.substring(startIndex, maskIndex);
        const results = resultsWithTailing.substring(
          0, resultsWithTailing.lastIndexOf(','));
        rows = JSON.parse(results);
        data = rows.map(this.processFn).join('\n') + '\n';
      }
      // Clear cached chunks.
      this.chunks = [latest.subarray(latest.indexOf(END_TAG) + END_TAG.length)];

      this.logger.debug(`Got ${rows.length} rows. Process time:`,
        Date.now() - this.stopwatch);
      this.stopwatch = Date.now();
      callback(null, data);
    } else {
      if (chunk.length < END_TAG.length) {// Update latest chunk for short chunk
        this.chunks[this.chunks.length - 1] = latest;
      } else {
        this.chunks.push(chunk);
      }
      callback();
    }
  }
}

module.exports = {
  getCleanCid,
  RestSearchStreamTransform,
};
