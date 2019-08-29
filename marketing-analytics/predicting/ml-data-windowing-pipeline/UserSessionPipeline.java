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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;

/**
 * Pipeline for converting Google Analytics BigQuery data into user Sessions.
 */
public class UserSessionPipeline {
  public static void main(String[] args) {
    UserSessionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(UserSessionPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    Instant startTime = DateUtil.parseStartDateStringToInstant(options.getStartDate());
    Instant endTime = DateUtil.parseEndDateStringToInstant(options.getEndDate());
    pipeline
        .apply("Read BigQuery GA Table",
            BigQueryIO.readTableRows().fromQuery(options.getInputBigQuerySQL()).usingStandardSql())
        .apply("MapGATableRowToSession", ParDo.of(new MapGATableRowToSession(
            options.getPredictionFactName(), options.getPredictionFactValues())))
        .apply(Filter.by((Session session) ->
            session.getVisitStartTime().compareTo(startTime) >= 0
            && session.getLastHitTime().compareTo(endTime) < 0))
        .apply(AvroIO.write(Session.class).to(
            options.getOutputSessionsAvroPrefix()).withSuffix(".avro"));
    pipeline.run();
  }
}
