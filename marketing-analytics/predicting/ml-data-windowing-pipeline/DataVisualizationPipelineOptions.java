// Copyright 2019 Google LLC
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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The options used to setup a UserSessionPipeline.
 */
public interface DataVisualizationPipelineOptions extends PipelineOptions {

  @Description("Location of the input user Sessions in AVRO format.")
  @Validation.Required
  String getInputAvroSessionsLocation();
  void setInputAvroSessionsLocation(String inputAvroSessionsLocation);

  @Description("Start date in dd/mm/yyyy format.")
  @Validation.Required
  String getStartDate();
  void setStartDate(String startDate);

  @Description("End date (not inclusive) in dd/mm/yyyy format.")
  @Validation.Required
  String getEndDate();
  void setEndDate(String endDate);

  @Description("Slide Length (seconds)")
  @Default.Long(86400L)
  @Validation.Required
  long getSlideTimeInSeconds();
  void setSlideTimeInSeconds(long slideTimeInSeconds);

  @Description("Minimum lookahead time (seconds)")
  @Default.Long(86400L)
  @Validation.Required
  long getMinimumLookaheadTimeInSeconds();
  void setMinimumLookaheadTimeInSeconds(long minimumLookaheadTimeInSeconds);

  @Description("Maximum lookahead time (seconds)")
  @Default.Long(604800L)
  @Validation.Required
  long getMaximumLookaheadTimeInSeconds();
  void setMaximumLookaheadTimeInSeconds(long maximumLookaheadTimeInSeconds);

  @Description("Set true to stop window generation after the first positive label per user.")
  @Default.Boolean(true)
  @Validation.Required
  boolean getStopOnFirstPositiveLabel();
  void setStopOnFirstPositiveLabel(boolean stopOnFirstPositiveLabel);

  @Description("Location to write the BigQuery Facts table.")
  @Validation.Required
  String getOutputBigQueryFactsTable();
  void setOutputBigQueryFactsTable(String outputBigQueryFactsTable);

  @Description("Location to write the BigQuery UserActivity table.")
  @Validation.Required
  String getOutputBigQueryUserActivityTable();
  void setOutputBigQueryUserActivityTable(String outputBigQueryUserActivityTable);
}
