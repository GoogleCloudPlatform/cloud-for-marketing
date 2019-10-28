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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.transform;

import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.LookbackWindow;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Session;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Parent class for mapping a user's sessions time into LookbackWindows.
 */
public class MapSortedSessionsIntoLookbackWindows extends DoFn<
    KV<String, List<Session>>, LookbackWindow> {
  protected ValueProvider<String> startTimeProvider;
  protected ValueProvider<String> endTimeProvider;
  protected ValueProvider<Long> lookbackGapInSecondsProvider;
  protected ValueProvider<Long> windowTimeInSecondsProvider;
  protected ValueProvider<Long> minimumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Long> maximumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Boolean> stopOnFirstPositiveLabelProvider;

  public MapSortedSessionsIntoLookbackWindows(
      ValueProvider<String> startTime,
      ValueProvider<String> endTime,
      ValueProvider<Long> lookbackGapInSeconds,
      ValueProvider<Long> windowTimeInSeconds,
      ValueProvider<Long> minimumLookaheadTimeInSeconds,
      ValueProvider<Long> maximumLookaheadTimeInSeconds,
      ValueProvider<Boolean> stopOnFirstPositiveLabel) {
    startTimeProvider = startTime;
    endTimeProvider = endTime;
    lookbackGapInSecondsProvider = lookbackGapInSeconds;
    windowTimeInSecondsProvider = windowTimeInSeconds;
    minimumLookaheadTimeInSecondsProvider = minimumLookaheadTimeInSeconds;
    maximumLookaheadTimeInSecondsProvider = maximumLookaheadTimeInSeconds;
    stopOnFirstPositiveLabelProvider = stopOnFirstPositiveLabel;
  }
}
