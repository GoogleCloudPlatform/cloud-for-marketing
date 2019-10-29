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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.AccumulatorOptions;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.AccumulatorType;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.FeatureAccumulatorFactory;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.transform.CreateAccumulatorOptionsFn;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.transform.CreateTableSchemaFn;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.transform.ExtractFeatureFn;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.LookbackWindow;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Pipeline to generate aggregated features from {@link LookbackWindow} avro objects based on the
 * input parameters.
 */
public class GenerateFeaturesPipeline {

  public static final String INTEGER = "INTEGER";
  public static final String STRING = "STRING";
  public static final String BOOL = "BOOL";
  public static final String NULLABLE = "NULLABLE";
  public static final String REQUIRED = "REQUIRED";

  public static void main(String[] args) {

    GenerateFeaturesPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(GenerateFeaturesPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    FeatureAccumulatorFactory factory = new FeatureAccumulatorFactory();

    PCollection<KV<String, AccumulatorOptions>> accumulatorOptions =
        getAllAccumulatorOptionsCollection(options, pipeline, factory);

    PCollectionView<Map<String, Iterable<AccumulatorOptions>>> accumulatorOptionsView =
        accumulatorOptions
            .apply(
                "Group accumulator options by column name",
                GroupByKey.<String, AccumulatorOptions>create())
            .apply(
                "Create accumulator options view",
                View.<String, Iterable<AccumulatorOptions>>asMap());

    PCollectionView<Map<String, String>> schemaView =
        accumulatorOptions
            .apply(
                "Create feature bq column names",
                ParDo.of(
                    new DoFn<KV<String, AccumulatorOptions>, Iterable<KV<String, String>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<String, AccumulatorOptions> e = c.element();
                        c.output(e.getValue().createFieldSchemas());
                      }
                    }))
            .apply(
                "Combine feature bq column names",
                Combine.globally(
                    new CombineFn<
                        Iterable<KV<String, String>>,
                        List<KV<String, String>>,
                        Iterable<KV<String, String>>>() {

                      @Override
                      public List<KV<String, String>> createAccumulator() {
                        return Lists.newArrayList();
                      }

                      @Override
                      public List<KV<String, String>> addInput(
                          List<KV<String, String>> schemas, Iterable<KV<String, String>> input) {
                        input.forEach(schemas::add);
                        return schemas;
                      }

                      @Override
                      public List<KV<String, String>> mergeAccumulators(
                          Iterable<List<KV<String, String>>> lists) {
                        return Lists.newArrayList(Iterables.concat(lists));
                      }

                      @Override
                      public Iterable<KV<String, String>> extractOutput(
                          List<KV<String, String>> schemas) {
                        schemas.add(KV.of("userId", STRING));
                        schemas.add(KV.of("startTime", INTEGER));
                        schemas.add(KV.of("endTime", INTEGER));
                        schemas.add(KV.of("effectiveDate", "DATETIME"));
                        schemas.add(KV.of("effectiveDateWeekOfYear", STRING));
                        schemas.add(KV.of("effectiveDateMonthOfYear", STRING));
                        schemas.add(KV.of("predictionLabel", BOOL));
                        return schemas;
                      }
                    }))
            .apply(
                "Create table schema",
                ParDo.of(new CreateTableSchemaFn(options.getFeatureDestinationTable())))
            .apply("Create table schema view", View.asSingleton());

    pipeline
        .apply(
            "Read Windowed Avro Facts",
            AvroIO.read(LookbackWindow.class)
                .from(options.getWindowedAvroLocation()))
        .apply(
            "Extract Features",
            ParDo.of(new ExtractFeatureFn(factory, accumulatorOptionsView))
                .withSideInputs(accumulatorOptionsView))
        .apply(
            BigQueryIO.writeTableRows()
                .to(options.getFeatureDestinationTable())
                .withSchemaFromView(schemaView)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    pipeline.run();
  }

  private static PCollection<KV<String, AccumulatorOptions>> getAllAccumulatorOptionsCollection(
      GenerateFeaturesPipelineOptions options,
      Pipeline pipeline,
      FeatureAccumulatorFactory factory) {
    PCollection<KV<String, AccumulatorOptions>> prop =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve proportions value from variables",
            options.getProportionsValueFromVariables(),
            "Create proportion accumulator options",
            AccumulatorType.PROPORTION);
    PCollection<KV<String, AccumulatorOptions>> freq =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve most frequent value from variables",
            options.getMostFreqValueFromVariables(),
            "Create most frequent accumulator options",
            AccumulatorType.MOST_FREQUENT);
    PCollection<KV<String, AccumulatorOptions>> sum =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve sum value from variables",
            options.getSumFromVariables(),
            "Create sum accumulator options",
            AccumulatorType.SUM);
    PCollection<KV<String, AccumulatorOptions>> average =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve average value from variables",
            options.getAverageValueFromVariables(),
            "Create average accumulator options",
            AccumulatorType.AVERAGE);
    PCollection<KV<String, AccumulatorOptions>> count =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve count value from variables",
            options.getCountValueFromVariables(),
            "Create count accumulator options",
            AccumulatorType.COUNT);
    PCollection<KV<String, AccumulatorOptions>> averageByTenure =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve average by tenure value from variables",
            options.getAverageByTenureValueFromVariables(),
            "Create average by tenure  accumulator options",
            AccumulatorType.AVERAGE_BY_TENURE);
    PCollection<KV<String, AccumulatorOptions>> recent =
        getAccumulatorOptionsCollection(
            pipeline,
            factory,
            "Retrieve recent value from variables",
            options.getRecentValueFromVariables(),
            "Create recent accumulator options",
            AccumulatorType.RECENT);

    return PCollectionList.of(prop)
        .and(freq)
        .and(sum)
        .and(average)
        .and(count)
        .and(averageByTenure)
        .and(recent)
        .apply("Combine accumulator options", Flatten.pCollections());
  }

  private static PCollection<KV<String, AccumulatorOptions>> getAccumulatorOptionsCollection(
      Pipeline pipeline,
      FeatureAccumulatorFactory factory,
      String createPCollectionStepDesc,
      ValueProvider<String> proportionsValueFromVariables,
      String createAccumulatorStepDesc,
      AccumulatorType proportion) {
    return pipeline
        .apply(
            createPCollectionStepDesc,
            Create.ofProvider(proportionsValueFromVariables, StringUtf8Coder.of()))
        .apply(
            createAccumulatorStepDesc,
            ParDo.of(new CreateAccumulatorOptionsFn(factory, proportion)));
  }
}
