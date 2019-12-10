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

/** Accumulator types for generating features in {@code GenerateFeaturesPipeline}. */
public enum AccumulatorType {
  PROPORTION(true),
  MOST_FREQUENT(false, true),
  SUM(false, true),
  AVERAGE(false, true),
  COUNT(false),
  AVERAGE_BY_TENURE(false /* valueListRequired */, true /* singleOutput */, true /* windowBased */),
  RECENT(false, true);

  private final boolean valueListRequired;
  private final boolean singleOutput;
  private final boolean windowBased;

  AccumulatorType(boolean valueListRequired) {
    this(valueListRequired, false, false);
  }

  AccumulatorType(boolean valueListRequired, boolean singleOutput) {
    this(valueListRequired, singleOutput, false);
  }

  AccumulatorType(boolean valueListRequired, boolean singleOutput, boolean windowBased) {
    this.valueListRequired = valueListRequired;
    this.singleOutput = singleOutput;
    this.windowBased = windowBased;
  }

  public boolean isValueListRequired() {
    return valueListRequired;
  }

  public boolean isSingleOutput() {
    return singleOutput;
  }

  public boolean isWindowBased() {
    return windowBased;
  }
}
