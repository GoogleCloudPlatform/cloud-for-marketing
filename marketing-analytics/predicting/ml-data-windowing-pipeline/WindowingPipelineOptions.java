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
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The options used to setup a WindowingPipeline.
 */
public interface WindowingPipelineOptions extends PipelineOptions {

  @Description("Location of the input user Sessions in AVRO format.")
  @Validation.Required
  ValueProvider<String> getInputAvroSessionsLocation();
  void setInputAvroSessionsLocation(ValueProvider<String> inputAvroSessionsLocation);

  @Description("Start date in dd/mm/yyyy format.")
  ValueProvider<String> getStartDate();
  void setStartDate(ValueProvider<String> startDate);

  @Description("End date in dd/mm/yyyy format.")
  ValueProvider<String> getEndDate();
  void setEndDate(ValueProvider<String> endDate);

  @Description("Minimum lookahead time (seconds)")
  @Default.Long(86400L)
  @Validation.Required
  ValueProvider<Long> getMinimumLookaheadTimeInSeconds();
  void setMinimumLookaheadTimeInSeconds(ValueProvider<Long> minimumLookaheadTimeInSeconds);

  @Description("Maximum lookahead time (seconds)")
  @Default.Long(604800L)
  @Validation.Required
  ValueProvider<Long> getMaximumLookaheadTimeInSeconds();
  void setMaximumLookaheadTimeInSeconds(ValueProvider<Long> maximumLookaheadTimeInSeconds);

  @Description("Lookback gap (seconds). Sessions within the lookback gap before an effective "
               + "date are not added to a LookbackWindow.")
  @Default.Long(86400L)
  @Validation.Required
  ValueProvider<Long> getLookbackGapInSeconds();
  void setLookbackGapInSeconds(ValueProvider<Long> lookbackGapInSeconds);


  @Description("Window Length (seconds)")
  @Default.Long(2592000L)
  @Validation.Required
  ValueProvider<Long> getWindowTimeInSeconds();
  void setWindowTimeInSeconds(ValueProvider<Long> windowTimeInSeconds);

  @Description("Slide Length (seconds)")
  @Default.Long(86400L)
  @Validation.Required
  ValueProvider<Long> getSlideTimeInSeconds();
  void setSlideTimeInSeconds(ValueProvider<Long> slideTimeInSeconds);

  @Description("Set true to stop window generation after the first positive label per user.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getStopOnFirstPositiveLabel();
  void setStopOnFirstPositiveLabel(ValueProvider<Boolean> stopOnFirstPositiveLabel);

  @Description("Location prefix to write the sliding lookback windows.")
  @Validation.Required
  ValueProvider<String> getOutputSlidingWindowAvroPrefix();
  void setOutputSlidingWindowAvroPrefix(ValueProvider<String> outputSlidingWindowAvroPrefix);

  @Description("Location prefix to write the session-based lookback windows.")
  @Validation.Required
  ValueProvider<String> getOutputSessionBasedWindowAvroPrefix();
  void setOutputSessionBasedWindowAvroPrefix(
      ValueProvider<String> outputSessionBasedWindowAvroPrefix);
}
