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

/** The options used to setup {@link GenerateFeaturesPipeline}. */
public interface GenerateFeaturesPipelineOptions extends PipelineOptions {

  @Description("Location to read the windowed avro objects.")
  @Validation.Required
  ValueProvider<String> getWindowedAvroLocation();

  void setWindowedAvroLocation(ValueProvider<String> windowedAvroLocation);

  @Description("Proportion of values from variables in facts in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getProportionsValueFromVariables();

  void setProportionsValueFromVariables(ValueProvider<String> value);

  @Description("Most frequent value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getMostFreqValueFromVariables();

  void setMostFreqValueFromVariables(ValueProvider<String> value);

  @Description("Average value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getAverageValueFromVariables();

  void setAverageValueFromVariables(ValueProvider<String> value);

  @Description("Average by tenure value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getAverageByTenureValueFromVariables();

  void setAverageByTenureValueFromVariables(ValueProvider<String> value);

  @Description("Sum value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getSumValueFromVariables();

  void setSumValueFromVariables(ValueProvider<String> value);

  @Description("Count value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getCountValueFromVariables();

  void setCountValueFromVariables(ValueProvider<String> value);

  @Description("Recent value from variables in facts table in lookback window.")
  @Validation.Required
  @Default.String("")
  ValueProvider<String> getRecentValueFromVariables();

  void setRecentValueFromVariables(ValueProvider<String> value);

  @Description("Feature BigQuery Destination.")
  @Validation.Required
  ValueProvider<String> getFeatureDestinationTable();
  void setFeatureDestinationTable(ValueProvider<String> featureDestinationTable);

  @Description("Set true to include predictionLabel in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getTrainMode();

  void setTrainMode(ValueProvider<Boolean> trainMode);

  @Description("Set true to include effectiveDate in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getShowEffectiveDate();

  void setShowEffectiveDate(ValueProvider<Boolean> showEffectiveDate);

  @Description("Set true to include startTime in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getShowStartTime();

  void setShowStartTime(ValueProvider<Boolean> showStartTime);

  @Description("Set true to include endTime in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getShowEndTime();

  void setShowEndTime(ValueProvider<Boolean> showEndTime);

  @Description("Set true to include effectiveDateWeekOfYear in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getShowEffectiveDateWeekOfYear();

  void setShowEffectiveDateWeekOfYear(ValueProvider<Boolean> showEffectiveDateWeekOfYear);

  @Description("Set true to include effectiveDateMonthOfYear in the output.")
  @Default.Boolean(true)
  @Validation.Required
  ValueProvider<Boolean> getShowEffectiveDateMonthOfYear();

  void setShowEffectiveDateMonthOfYear(ValueProvider<Boolean> showEffectiveDateMonthOfYear);
}
