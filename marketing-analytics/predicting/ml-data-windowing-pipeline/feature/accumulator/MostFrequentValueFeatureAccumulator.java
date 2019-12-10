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

import static java.util.Comparator.reverseOrder;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;

/**
 * Feature accumulator to extract most frequent value among the values provided of a fact. Example:
 * Extract the most frequent value of the fact 'city' as a feature.
 */
public class MostFrequentValueFeatureAccumulator extends FeatureAccumulator {

  private final Bag<String> valueBag;

  public MostFrequentValueFeatureAccumulator(
      String column, ImmutableMap<String, String> valueToFeatureName, String defaultValue) {
    super(column, valueToFeatureName, defaultValue);
    this.valueBag = new HashBag<>();
  }

  @Override
  public void accumulate(String value) {
    if (getValueToFeatureName().containsKey(getFactName())
        || getValueToFeatureName().containsKey(value)) {
      valueBag.add(value);
    } else if (!Strings.isNullOrEmpty(getDefaultValue())) {
      valueBag.add(getDefaultValue());
    }
  }

  @Override
  public Map<String, Object> getFeatures() {

    if (getValueToFeatureName().isEmpty()) {
      return ImmutableMap.of();
    }

    Optional<String> valueWithMaxCount =
        valueBag.stream()
            .max(
                Comparator.<String>comparingInt(valueBag::getCount)
                    .thenComparing(s -> s, reverseOrder()));
    String featureName = getValueToFeatureName().values().iterator().next();
    return ImmutableMap.of(featureName, valueWithMaxCount.orElse(""));
  }
}
