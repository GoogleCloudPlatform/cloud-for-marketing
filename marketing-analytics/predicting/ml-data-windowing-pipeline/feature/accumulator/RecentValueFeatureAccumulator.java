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

import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Feature accumulator to extract the most recent value of a fact. */
public class RecentValueFeatureAccumulator extends FeatureAccumulator {

  private String value;

  public RecentValueFeatureAccumulator(
      String column, ImmutableMap<String, String> values, String defaultValue) {
    super(column, values, defaultValue);
  }

  @Override
  public void accumulate(String value) {
    if (getValueToFeatureName().containsKey(getFactName())
        || getValueToFeatureName().containsKey(value)) {
      this.value = value;
    } else if (!Strings.isNullOrEmpty(getDefaultValue())) {
      this.value = getDefaultValue();
    }
  }

  @Override
  public Map<String, Object> getFeatures() {
    String featureName = getValueToFeatureName().values().iterator().next();
    return ImmutableMap.of(featureName, nullToEmpty(value));
  }
}
