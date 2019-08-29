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

import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.LookbackWindow;
import java.util.Map;

/**
 * An object that accumulates values to generate feature or features based on {@link LookbackWindow}
 * for a ML-ready dataset.
 */
public abstract class WindowBasedFeatureAccumulator extends FeatureAccumulator {

  private LookbackWindow window;

  public WindowBasedFeatureAccumulator(
      String column, Map<String, String> values, String defaultValue) {
    super(column, values, defaultValue);
  }

  @Override
  public void accumulate(String value) {
    if (window == null) {
      throw new IllegalStateException("LookbackWindow is not initialized.");
    }
  }

  public LookbackWindow getWindow() {
    return window;
  }

  public void setWindow(LookbackWindow window) {
    this.window = window;
  }
}
