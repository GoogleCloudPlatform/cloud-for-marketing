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
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Fact;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Converts a Fact to a BigQuery TableRow.
 */
public class MapFactToTableRow extends DoFn<Fact, TableRow> {

  // Returns the table schema for Fact TableRows.
  public static TableSchema getTableSchema() {
    TableSchema schema = new TableSchema();
    schema.setFields(Arrays.asList(
        new TableFieldSchema().setName("sessionId").setType("STRING"),
        new TableFieldSchema().setName("userId").setType("STRING"),
        new TableFieldSchema().setName("timeInMillis").setType("INTEGER"),
        new TableFieldSchema().setName("name").setType("STRING"),
        new TableFieldSchema().setName("value").setType("STRING"),
        new TableFieldSchema().setName("hasPositiveLabel").setType("BOOLEAN")));
    return schema;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Fact fact = context.element();
    TableRow tablerow = new TableRow();
    tablerow.set("sessionId", fact.getSessionId());
    tablerow.set("userId", fact.getUserId());
    tablerow.set("timeInMillis", fact.getTime().getMillis());
    tablerow.set("name", fact.getName());
    tablerow.set("value", fact.getValue());
    tablerow.set("hasPositiveLabel", fact.getHasPositiveLabel());
    context.output(tablerow);
  }
}
