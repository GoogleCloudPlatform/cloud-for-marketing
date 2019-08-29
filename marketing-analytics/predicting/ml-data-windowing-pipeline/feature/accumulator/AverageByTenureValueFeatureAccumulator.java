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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator;

import com.google.common.collect.ImmutableMap;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.LookbackWindow;
import java.util.Map;
import org.joda.time.Duration;

/**
 * Feature accumulator to extract the average by the tenure value of a fact. Example: Extract the
 * average value of the fact 'page_views' as a feature.
 */
public class AverageByTenureValueFeatureAccumulator extends WindowBasedFeatureAccumulator {

  private double sum;

  public AverageByTenureValueFeatureAccumulator(
      String column, ImmutableMap<String, String> values, String defaultValue) {
    super(column, values, defaultValue);
  }

  @Override
  public Map<String, Object> getFeatures() {
    String featureName = getValueToFeatureName().values().iterator().next();
    return ImmutableMap.of(featureName, roundedFeatureValue(sum / Math.max(getTenure(), 1)));
  }

  @Override
  public void accumulate(String value) {
    super.accumulate(value);
    sum += Double.parseDouble(value);
  }

  // Window size or the period the instance has been active in the window whichever is lesser.
  // Example: Window size is 30 days but the instance has only activity for the last 10 days of that
  // window. For this case, tenure is 10 days.
  private long getTenure() {
    final LookbackWindow window = getWindow();

    // The difference of window's endTime and startTime.
    long windowSizeInDays =
        new Duration(window.getStartTime(), window.getEndTime()).getStandardDays();
    long activityPeriodInDays =
        new Duration(
                window.getFirstActivityTime().getMillis(), window.getEffectiveDate().getMillis())
            .getStandardDays();

    return Math.min(windowSizeInDays, activityPeriodInDays);
  }
}
