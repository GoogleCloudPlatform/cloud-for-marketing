// Copyright 2021 Google Inc.
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
 * @fileoverview Interface for YouTube Reporting.
 */

'use strict';

const {
  api: {
    youtube: {
      ListChannelsConfig,
      ListVideosConfig,
      ListCommentThreadsConfig,
      ListPlaylistConfig,
      ListSearchConfig,
      YouTube,
    }
  }
} = require('@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/**
 * Types of implemented YouTube API.
 * @enum {string}
 */
const ReportTargets = Object.freeze({
  CHANNEL: 'CHANNEL',
  VIDEO: 'VIDEO',
  COMMENT: 'COMMENT',
  PLAYLIST: 'PLAYLIST',
  SEARCH: 'SEARCH',
});

/**
 * Configuration for YouTube API.
 * @typedef {!(
 *   ListChannelsConfig
 *   | ListVideosConfig
 *   | ListCommentThreadsConfig
 *   | ListPlaylistConfig
 *   | ListSearchConfig
 * )}
 */
let ListConfig;

/** YouTube Report class. */
class YouTubeReport extends Report {

  constructor(config, yt = new YouTube()) {
    super(config);
    this.yt = yt;
  }

  /** @override */
  generate(parameters) {
    return Promise.resolve();
  }

  /** @override */
  isReady(parameters) {
    return Promise.resolve(true);
  }

  /** @override */
  isAsynchronous() {
    return false;
  }

  /**
   * Map the supported YouTube API functions based on the source
   * YouTubeReportConfig.
   * @param  {string} target
   * @return {function(
   *   ListConfig): function(ListConfig): !Promise<!Array<!Object>>}
   * @private
   */
  getHandleFunction_(target) {
    switch (target) {
      case ReportTargets.CHANNEL:
        return (reportQuery) => {return this.yt.listChannels(reportQuery)};
      case ReportTargets.VIDEO:
        return (reportQuery) => {return this.yt.listVideos(reportQuery)};
      case ReportTargets.COMMENT:
        return (reportQuery) => {
          return this.yt.listCommentThreads(reportQuery);
        };
      case ReportTargets.PLAYLIST:
        return (reportQuery, resultLimit) => {
          return this.yt.listPlaylists(reportQuery, resultLimit);
        };
      case ReportTargets.SEARCH:
        return (reportQuery, resultLimit) => {
          return this.yt.listSearchResults(reportQuery, resultLimit);
        };
      default:
        throw new Error(
            `Fail to find the API for the target: ${target}`);
    }
  }

  /**
   * Get content from YouTube API based on the source YouTubeReportConfig.
   * @override
   */
  async getContent(parameters) {
    const handler = this.getHandleFunction_(this.config.target);
    const items = await handler(
      this.config.reportQuery,
      this.config.resultLimit);
    return items.map(JSON.stringify).join('\n');
  }
}

module.exports = {YouTubeReport};
