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

import java.math.BigDecimal;
import java.util.Map;

/**
 * An object that accumulates values to generate feature or features for a ML-ready dataset.
 *
 * <p>{@code Example: factName = "browser" valueToFeatureName = [("Chrome",
 * "PROPORTION_browser_Chrome"),("Safari", "PROPORTION_browser_Safari")]}
 */
public abstract class FeatureAccumulator {

  // Map of value and its corresponding feature name.
  private final Map<String, String> valueToFeatureName;

  // Fact that the feature is based on.
  private final String factName;

  // Value to accumulate if value is not in the valueToFeatureName map.
  private final String defaultValue;

  protected FeatureAccumulator(
      String factName, Map<String, String> valueToFeatureName, String defaultValue) {

    this.factName = factName;
    this.valueToFeatureName = valueToFeatureName;
    this.defaultValue = defaultValue;
  }

  /** Accumulates value of a fact. */
  public abstract void accumulate(String value);

  /** Returns a map of features and their corresponding accumulated value. */
  public abstract Map<String, Object> getFeatures();

  protected String getFactName() {
    return factName;
  }

  protected Map<String, String> getValueToFeatureName() {
    return valueToFeatureName;
  }

  protected String getDefaultValue() {
    return defaultValue;
  }

  protected static double roundedFeatureValue(double number) {
    return new BigDecimal(number).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
}
