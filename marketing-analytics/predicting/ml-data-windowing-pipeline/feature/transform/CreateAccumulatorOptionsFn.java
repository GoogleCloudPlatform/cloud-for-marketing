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

import com.google.common.collect.Multimap;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.AccumulatorOptions;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.AccumulatorType;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator.FeatureAccumulatorFactory;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Function to create @{link {@link AccumulatorOptions}} based string parameter. */
public class CreateAccumulatorOptionsFn extends DoFn<String, KV<String, AccumulatorOptions>> {
  private final FeatureAccumulatorFactory factory;
  private final AccumulatorType accumulatorType;

  public CreateAccumulatorOptionsFn(
      FeatureAccumulatorFactory factory, AccumulatorType accumulatorType) {
    this.factory = factory;
    this.accumulatorType = accumulatorType;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Multimap<String, AccumulatorOptions> accumulators =
        factory.createAccumulatorOptions(accumulatorType, c.element());
    for (Map.Entry<String, AccumulatorOptions> accumulatorMapEntry : accumulators.entries()) {
      c.output(KV.of(accumulatorMapEntry.getKey(), accumulatorMapEntry.getValue()));
    }
  }
}
