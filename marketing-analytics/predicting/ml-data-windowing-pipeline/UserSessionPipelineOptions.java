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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The options used to setup a UserSessionPipeline.
 */
public interface UserSessionPipelineOptions extends PipelineOptions {
  @Description("Input BigQuery SQL command for extracting GA Sessions columns.")
  @Validation.Required
  ValueProvider<String> getInputBigQuerySQL();
  void setInputBigQuerySQL(ValueProvider<String> inputBigQuerySQL);

  @Description("Name of the Fact with the prediction target.")
  @Validation.Required
  ValueProvider<String> getPredictionFactName();
  void setPredictionFactName(ValueProvider<String> predictionFactName);

  @Description("Comma separated list of target values for the prediction Fact.")
  @Validation.Required
  ValueProvider<String> getPredictionFactValues();
  void setPredictionFactValues(ValueProvider<String> predictionFactValues);

  @Description("Location prefix to write all user Sessions in AVRO format.")
  @Validation.Required
  ValueProvider<String> getOutputSessionsAvroPrefix();
  void setOutputSessionsAvroPrefix(ValueProvider<String> outputSessionsAvroPrefix);
}
