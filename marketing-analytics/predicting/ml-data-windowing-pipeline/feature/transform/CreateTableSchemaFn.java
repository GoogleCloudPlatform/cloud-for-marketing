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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Function to create a schema json string based on the map of feature column name and type. */
public class CreateTableSchemaFn extends DoFn<Iterable<KV<String, String>>, Map<String, String>> {

  public static final String NULLABLE = "NULLABLE";
  private final String tableOutput;

  public CreateTableSchemaFn(String tableOutput) {
    this.tableOutput = tableOutput;
  }

  @ProcessElement
  public void apply(ProcessContext context) {
    TableSchema tableSchema = new TableSchema();

    Iterable<KV<String, String>> fieldKVs = context.element();
    List<TableFieldSchema> tableFields = new ArrayList<>();
    fieldKVs.forEach(
        field -> {
          tableFields.add(
              new TableFieldSchema()
                  .setName(field.getKey())
                  .setType(field.getValue())
                  .setMode(NULLABLE));
        });
    tableSchema.setFields(tableFields);

    String jsonSchema;
    try {
      jsonSchema = Transport.getJsonFactory().toString(tableSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error creating bq schema", e);
    }
    context.output(ImmutableMap.of(tableOutput, jsonSchema));
  }
}
