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

import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Session;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapGATableRowToSession;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.ValidateGATableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Pipeline for converting Google Analytics BigQuery data into user Sessions.
 */
public class UserSessionPipeline {
  public static void main(String[] args) {
    UserSessionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(UserSessionPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Read BigQuery GA Table",
            BigQueryIO.readTableRows().withTemplateCompatibility().fromQuery(
                options.getInputBigQuerySQL()).usingStandardSql().withoutValidation())
        .apply("ValidateGATableRow", ParDo.of(new ValidateGATableRow()))
        .apply("MapGATableRowToSession", ParDo.of(new MapGATableRowToSession(
                options.getFactsToExtract(),
                options.getPredictionFactName(),
                options.getPredictionFactValues())))
        .apply(AvroIO.write(Session.class).to(
            options.getOutputSessionsAvroPrefix()).withSuffix(".avro"));
    pipeline.run();
  }
}
