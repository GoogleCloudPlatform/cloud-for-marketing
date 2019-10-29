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
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapFactToTableRow;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapSessionToFacts;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapSortedSessionsToUserActivities;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapUserActivityToTableRow;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.MapUserIdToSession;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform.SortSessionsByTime;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Pipeline for outputing the Facts and UserActivity data tables from the input user Sessions.
 */
public class DataVisualizationPipeline {
  public static void main(String[] args) {
    DataVisualizationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(
            DataVisualizationPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Session> sessions = pipeline
        .apply(AvroIO.read(Session.class).from(options.getInputAvroSessionsLocation()));

    // Output Facts BigQuery table.
    sessions
        .apply("MapSessionToFacts", ParDo.of(new MapSessionToFacts()))
        .apply("MapFactToTableRow", ParDo.of(new MapFactToTableRow()))
        .apply("WriteFactsToBigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getOutputBigQueryFactsTable())
                .withoutValidation()
                .withSchema(MapFactToTableRow.getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    // Output UserActivity BigQuery table.
    sessions
        .apply("MapUserIdToSession", ParDo.of(new MapUserIdToSession()))
        .apply(GroupByKey.<String, Session>create())
        .apply("SortSessionsByTime", ParDo.of(new SortSessionsByTime()))
        .apply(
            "MapSortedSessionsToUserActivities",
            ParDo.of(
                new MapSortedSessionsToUserActivities(
                    options.getStartDate(),
                    options.getEndDate(),
                    options.getSlideTimeInSeconds(),
                    options.getMinimumLookaheadTimeInSeconds(),
                    options.getMaximumLookaheadTimeInSeconds(),
                    options.getStopOnFirstPositiveLabel())))
        .apply("MapUserActivityToTableRow", ParDo.of(new MapUserActivityToTableRow()))
        .apply(
            "WriteUserActivitiesToBigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getOutputBigQueryUserActivityTable())
                .withoutValidation()
                .withSchema(MapUserActivityToTableRow.getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    pipeline.run();
  }
}
