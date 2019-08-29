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
 * The options used to setup a WindowingPipeline.
 */
public interface WindowingPipelineOptions extends PipelineOptions {

  @Description("Location of the input user Sessions in AVRO format.")
  @Validation.Required
  String getInputAvroSessionsLocation();
  void setInputAvroSessionsLocation(String inputAvroSessionsLocation);

  @Description("Start date in dd/mm/yyyy format.")
  String getStartDate();
  void setStartDate(String startDate);

  @Description("End date in dd/mm/yyyy format.")
  String getEndDate();
  void setEndDate(String endDate);

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

  @Description("Lookback gap (seconds). Sessions within the lookback gap before an effective "
               + "date are not added to a LookbackWindow.")
  @Default.Long(86400L)
  @Validation.Required
  long getLookbackGapInSeconds();
  void setLookbackGapInSeconds(long lookbackGapInSeconds);


  @Description("Window Length (seconds)")
  @Default.Long(2592000L)
  @Validation.Required
  long getWindowTimeInSeconds();
  void setWindowTimeInSeconds(long windowTimeInSeconds);

  @Description("Slide Length (seconds)")
  @Default.Long(86400L)
  @Validation.Required
  long getSlideTimeInSeconds();
  void setSlideTimeInSeconds(long slideTimeInSeconds);

  @Description("Set true to stop window generation after the first positive label per user.")
  @Default.Boolean(true)
  @Validation.Required
  boolean getStopOnFirstPositiveLabel();
  void setStopOnFirstPositiveLabel(boolean stopOnFirstPositiveLabel);

  @Description("Location prefix to write the sliding lookback windows.")
  @Validation.Required
  String getOutputSlidingWindowAvroPrefix();
  void setOutputSlidingWindowAvroPrefix(String outputSlidingWindowAvroPrefix);

  @Description("Location prefix to write the session-based lookback windows.")
  @Validation.Required
  String getOutputSessionBasedWindowAvroPrefix();
  void setOutputSessionBasedWindowAvroPrefix(String outputSessionBasedWindowAvroPrefix);
}
