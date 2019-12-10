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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableMap;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Field;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;

/** Function to create a schema json string based on the map of feature column name and type. */
public class CreateTableSchemaFn extends DoFn<Iterable<Field>, Map<String, String>> {

  public static final String NULLABLE = "NULLABLE";
  public static final String INTEGER = "INTEGER";
  public static final String STRING = "STRING";
  public static final String BOOL = "BOOL";
  public static final String REQUIRED = "REQUIRED";

  private final ValueProvider<String> tableOutput;
  private final ValueProvider<Boolean> trainMode;
  private final ValueProvider<Boolean> showEffectiveDate;
  private final ValueProvider<Boolean> showStartTime;
  private final ValueProvider<Boolean> showEndTime;
  private final ValueProvider<Boolean> showEffectiveDateWeekOfYear;
  private final ValueProvider<Boolean> showEffectiveDateMonthOfYear;

  public CreateTableSchemaFn(
      ValueProvider<String> tableOutput,
      ValueProvider<Boolean> trainMode,
      ValueProvider<Boolean> showEffectiveDate,
      ValueProvider<Boolean> showStartTime,
      ValueProvider<Boolean> showEndTime,
      ValueProvider<Boolean> showEffectiveDateWeekOfYear,
      ValueProvider<Boolean> showEffectiveDateMonthOfYear) {
    this.tableOutput = tableOutput;
    this.trainMode = trainMode;
    this.showEffectiveDate = showEffectiveDate;
    this.showEffectiveDateMonthOfYear = showEffectiveDateMonthOfYear;
    this.showEffectiveDateWeekOfYear = showEffectiveDateWeekOfYear;
    this.showEndTime = showEndTime;
    this.showStartTime = showStartTime;
  }

  @ProcessElement
  public void apply(ProcessContext context) {
    TableSchema tableSchema = new TableSchema();

    Iterable<Field> fields = context.element();
    List<TableFieldSchema> tableFields = new ArrayList<>();

    fields.forEach(
        field -> {
          tableFields.add(
              createTableFieldSchema(field.getName(), field.getType(), field.getDescription()));
        });

    tableFields.add(createTableFieldSchema("userId", STRING));
    if (showStartTime.get()) {
      tableFields.add(createTableFieldSchema("startTime", INTEGER));
    }
    if (showEndTime.get()) {
      tableFields.add(createTableFieldSchema("endTime", INTEGER));
    }
    if (showEffectiveDate.get()) {
      tableFields.add(createTableFieldSchema("effectiveDate", "DATETIME"));
    }
    if (showEffectiveDateWeekOfYear.get()) {
      tableFields.add(createTableFieldSchema("effectiveDateWeekOfYear", STRING));
    }
    if (showEffectiveDateMonthOfYear.get()) {
      tableFields.add(createTableFieldSchema("effectiveDateMonthOfYear", STRING));
    }
    if (trainMode.get()) {
      tableFields.add(createTableFieldSchema("predictionLabel", BOOL));
    }

    tableSchema.setFields(tableFields);

    String jsonSchema;
    try {
      jsonSchema = Transport.getJsonFactory().toString(tableSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error creating bq schema", e);
    }
    context.output(ImmutableMap.of(tableOutput.get(), jsonSchema));
  }

  private static TableFieldSchema createTableFieldSchema(String name, String type) {

    return createTableFieldSchema(name, type, null);
  }

  private static TableFieldSchema createTableFieldSchema(
      String name, String type, String description) {
    return new TableFieldSchema()
        .setName(name)
        .setType(type)
        .setMode(NULLABLE)
        .setDescription(description);
  }
}
