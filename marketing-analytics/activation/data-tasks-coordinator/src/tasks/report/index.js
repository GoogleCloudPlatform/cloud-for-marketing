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
 * @fileoverview Hosts all Reports used by Sentinel ReportTask.
 */

'use strict';

const {Report, ReportConfig} = require('./base_report.js');
const {CampaignManagerReport} = require('./campaign_manager_report.js');
const {DoubleClickSearchReport} = require('./doubleclick_search_report.js');
const {DoubleClickBidManagerReport} =
    require('./doubleclick_bidmanager_report.js');
const {GoogleAdsReport} = require('./googleads_report.js');
const {YouTubeReport} = require('./youtube_report.js');

/**
 * All reports supported by ReportTask.
 * @const {!Object<string,!Report>}
 */
const REPORTING_FACTORY = Object.freeze({
  'CM': CampaignManagerReport,
  'SA360': DoubleClickSearchReport,
  'DV360': DoubleClickBidManagerReport,
  'ADS': GoogleAdsReport,
  'YT': YouTubeReport,
});

/**
 * Gets the report implementation of the target system.
 * @param {!ReportConfig} reportConfig Report target system name.
 * @return {!Report} An implementation of the Report for the given system.
 */
const buildReport = (reportConfig) => {
  const report = REPORTING_FACTORY[reportConfig.target];
  if (report) return new report(reportConfig.config);
  throw new Error(`Fail to find Report for: ${reportConfig.target}`);
};

module.exports = {
  Report,
  ReportConfig,
  buildReport,
};
