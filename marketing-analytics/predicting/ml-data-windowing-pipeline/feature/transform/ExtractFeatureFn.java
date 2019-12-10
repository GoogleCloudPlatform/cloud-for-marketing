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

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.AccumulatorOptions;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.FeatureAccumulator;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.FeatureAccumulatorFactory;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.WindowBasedFeatureAccumulator;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Fact;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.LookbackWindow;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Session;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Function to extract features from the {@link LookbackWindow} based on a map of {@link
 * AccumulatorOptions}.
 */
public class ExtractFeatureFn extends DoFn<LookbackWindow, TableRow> {

  public static final String USER_ID = "userId";
  public static final String START_TIME = "startTime";
  public static final String END_TIME = "endTime";
  public static final String EFFECTIVE_DATE_WEEK_OF_YEAR = "effectiveDateWeekOfYear";
  public static final String EFFECTIVE_DATE_MONTH_OF_YEAR = "effectiveDateMonthOfYear";
  public static final String PREDICTION_LABEL = "predictionLabel";
  public static final String EFFECTIVE_DATE_WEEK_OF_YEAR_SUFFIX = "W";
  public static final String EFFECTIVE_DATE_MONTH_OF_YEAR_SUFFIX = "M";
  public static final String EFFECTIVE_DATE = "effectiveDate";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormat.forPattern("YYYY-MM-dd hh:mm:ss");
  ;
  private final FeatureAccumulatorFactory featureAccumulatorFactory;

  // View map of Fact Name and List of AccumulatorOptions that needs to be aggregated.
  private final PCollectionView<Map<String, Iterable<AccumulatorOptions>>> accumulatorOptionsView;
  private final ValueProvider<Boolean> trainMode;
  private final ValueProvider<Boolean> showEffectiveDate;
  private final ValueProvider<Boolean> showStartTime;
  private final ValueProvider<Boolean> showEndTime;
  private final ValueProvider<Boolean> showEffectiveDateWeekOfYear;
  private final ValueProvider<Boolean> showEffectiveDateMonthOfYear;

  public ExtractFeatureFn(
      FeatureAccumulatorFactory featureAccumulatorFactory,
      PCollectionView<Map<String, Iterable<AccumulatorOptions>>> accumulatorOptionsView,
      ValueProvider<Boolean> trainMode,
      ValueProvider<Boolean> showEffectiveDate,
      ValueProvider<Boolean> showStartTime,
      ValueProvider<Boolean> showEndTime,
      ValueProvider<Boolean> showEffectiveDateWeekOfYear,
      ValueProvider<Boolean> showEffectiveDateMonthOfYear) {
    this.featureAccumulatorFactory = featureAccumulatorFactory;
    this.accumulatorOptionsView = accumulatorOptionsView;
    this.trainMode = trainMode;
    this.showEffectiveDate = showEffectiveDate;
    this.showEffectiveDateMonthOfYear = showEffectiveDateMonthOfYear;
    this.showEffectiveDateWeekOfYear = showEffectiveDateWeekOfYear;
    this.showEndTime = showEndTime;
    this.showStartTime = showStartTime;
  }

  /**
   * Processes {@link LookbackWindow} to perform feature aggregation and output a {@link TableRow}.
   */
  @ProcessElement
  public void processElement(ProcessContext context) {
    LookbackWindow window = context.element();

    Multimap<String, FeatureAccumulator> accumulators =
        initAccumulators(window, context.sideInput(accumulatorOptionsView));

    for (Session session : window.getSessions()) {
      for (Fact fact : session.getFacts()) {
        for (FeatureAccumulator featureAccumulator : accumulators.get(fact.getName())) {
          featureAccumulator.accumulate(fact.getValue());
        }
      }
    }
    context.output(createTableRow(window, accumulators));
  }

  private TableRow createTableRow(
      LookbackWindow window, Multimap<String, FeatureAccumulator> accumulators) {
    TableRow row = new TableRow();
    row.set(USER_ID, window.getUserId());
    if (showStartTime.get()) {
      row.set(START_TIME, window.getStartTime().getMillis());
    }
    if (showEndTime.get()) {
      row.set(END_TIME, window.getEndTime().getMillis());
    }
    if (showEffectiveDate.get()) {
      row.set(EFFECTIVE_DATE, DATE_TIME_FORMATTER.print(window.getEffectiveDate()));
    }
    if (showEffectiveDateWeekOfYear.get()) {
      row.set(
          EFFECTIVE_DATE_WEEK_OF_YEAR,
          window.getEffectiveDate().toDateTime().getWeekOfWeekyear()
              + EFFECTIVE_DATE_WEEK_OF_YEAR_SUFFIX);
    }
    if (showEffectiveDateMonthOfYear.get()) {
      row.set(
          EFFECTIVE_DATE_MONTH_OF_YEAR,
          window.getEffectiveDate().toDateTime().getMonthOfYear()
              + EFFECTIVE_DATE_MONTH_OF_YEAR_SUFFIX);
    }
    if (trainMode.get()) {
      row.set(PREDICTION_LABEL, window.getPredictionLabel());
    }

    for (FeatureAccumulator featureAccumulator : accumulators.values()) {
      featureAccumulator.getFeatures().forEach(row::set);
    }
    return row;
  }

  private Multimap<String, FeatureAccumulator> initAccumulators(
      LookbackWindow window, Map<String, Iterable<AccumulatorOptions>> map) {

    ImmutableMultimap.Builder<String, FeatureAccumulator> res = ImmutableMultimap.builder();

    for (Map.Entry<String, Iterable<AccumulatorOptions>> e : map.entrySet()) {
      e.getValue()
          .forEach(
              b -> {
                FeatureAccumulator accumulator = featureAccumulatorFactory.createAccumulator(b);
                if (accumulator instanceof WindowBasedFeatureAccumulator) {
                  ((WindowBasedFeatureAccumulator) accumulator).setWindow(window);
                }
                res.put(e.getKey(), accumulator);
              });
    }
    return res.build();
  }
}
