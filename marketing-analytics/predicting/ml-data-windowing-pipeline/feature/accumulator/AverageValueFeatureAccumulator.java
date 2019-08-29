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
import java.util.Map;

/**
 * Feature accumulator to extract the average value of a fact. Example: Extract the average value of
 * the fact 'page_views' as a feature.
 */
public class AverageValueFeatureAccumulator extends FeatureAccumulator {

  private double sum;
  private int count;

  public AverageValueFeatureAccumulator(
      String column, ImmutableMap<String, String> valueToFeatureName, String defaultValue) {
    super(column, valueToFeatureName, defaultValue);
  }

  @Override
  public void accumulate(String value) {
    count++;
    try {
      sum += Double.parseDouble(value);
    } catch (NumberFormatException e) {
      // Ignore invalid value.
    }
  }

  @Override
  public Map<String, Object> getFeatures() {
    String columnName = getValueToFeatureName().values().iterator().next();
    return ImmutableMap.of(columnName, roundedFeatureValue(sum / Math.max(count, 1)));
  }
}
