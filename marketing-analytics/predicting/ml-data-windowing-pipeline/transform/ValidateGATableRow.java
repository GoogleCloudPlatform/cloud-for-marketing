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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Validates the BigQuery TableRows contains required field names.
 */
public class ValidateGATableRow extends DoFn<TableRow, TableRow> {
  public ValidateGATableRow() {
  }

  // Converts BigQuery TableRows from Google Analytics to Sessions.
  @ProcessElement
  public void processElement(ProcessContext context) {
    TableRow tablerow = context.element();
    if (!tablerow.containsKey("fullVisitorId")
        || !tablerow.containsKey("visitId")
        || !tablerow.containsKey("visitStartTime")) {
      throw new IllegalArgumentException(String.format(
          "Input tablerow [%s] is missing at least one of the following required fields: %s",
          tablerow, "fullVisitorId, visitId or visitStartTime"));
    }
    context.output(tablerow);
  }
}
