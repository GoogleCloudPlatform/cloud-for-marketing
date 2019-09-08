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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.UserActivity;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Converts a UserActivity to a BigQuery TableRow if the user has had some activity as of the
 * UserActivty's snapshotTime.
 */
public class MapUserActivityToTableRow extends DoFn<UserActivity, TableRow> {

  // Returns the table schema for UserActivity TableRows.
  public static TableSchema getTableSchema() {
    TableSchema schema = new TableSchema();
    schema.setFields(Arrays.asList(
        new TableFieldSchema().setName("userId").setType("STRING"),
        new TableFieldSchema().setName("hasPositiveLabel").setType("BOOLEAN"),
        new TableFieldSchema().setName("daysSinceStartDate").setType("INTEGER"),
        new TableFieldSchema().setName("daysSinceFirstActivity").setType("INTEGER"),
        new TableFieldSchema().setName("daysSinceLatestActivity").setType("INTEGER"),
        new TableFieldSchema().setName("snapshotTimeInMillis").setType("INTEGER")));
    return schema;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    UserActivity userActivity = context.element();
    if (userActivity.getDurationSinceFirstActivity() == null) {
      return;
    }
    TableRow tablerow = new TableRow();
    tablerow.set("userId", userActivity.getUserId());
    tablerow.set("hasPositiveLabel", userActivity.getHasPositiveLabel());
    tablerow.set("daysSinceStartDate", userActivity.getDurationSinceStartDate().getStandardDays());
    tablerow.set(
        "daysSinceFirstActivity", userActivity.getDurationSinceFirstActivity().getStandardDays());
    tablerow.set(
        "daysSinceLatestActivity", userActivity.getDurationSinceLatestActivity().getStandardDays());
    tablerow.set("snapshotTimeInMillis", userActivity.getSnapshotTime().getMillis());
    context.output(tablerow);
  }
}
