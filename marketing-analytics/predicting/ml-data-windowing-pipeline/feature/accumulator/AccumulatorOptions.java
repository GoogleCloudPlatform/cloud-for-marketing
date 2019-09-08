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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import org.apache.beam.sdk.values.KV;

/** Options used to setup {@link FeatureAccumulator} for feature generation. */
@AutoValue
public abstract class AccumulatorOptions implements Serializable {

  private static final long serialVersionUID = -7598696562228570310L;

  static Builder builder() {
    return new AutoValue_AccumulatorOptions.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setValueToFeatureName(ImmutableMap<String, String> valueToFeatureName);

    abstract Builder setColumn(String column);

    abstract Builder setType(String type);

    abstract Builder setSchemaType(String schemaType);

    abstract Builder setDefaultValue(String defaultValue);

    abstract AccumulatorOptions build();
  }

  abstract ImmutableMap<String, String> valueToFeatureName();

  abstract String column();

  abstract String type();

  abstract String schemaType();

  abstract String defaultValue();

  /**
   * Creates a list of pair of feature column name and feature column type based on the
   * valueToFeatureName values.
   */
  public ImmutableList<KV<String, String>> createFieldSchemas() {
    return valueToFeatureName().values().stream()
        .distinct()
        .map(featureName -> KV.of(featureName, schemaType()))
        .collect(toImmutableList());
  }
}
