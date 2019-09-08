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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Parent class for mapping a user's sessions time into LookbackWindows.
 */
public class MapSortedSessionsIntoLookbackWindows extends DoFn<
    KV<String, List<Session>>, LookbackWindow> {
  protected Instant startTime;
  protected Instant endTime;
  protected Duration lookbackGapDuration;
  protected Duration windowDuration;
  protected Duration minimumLookaheadDuration;
  protected Duration maximumLookaheadDuration;
  protected boolean stopOnFirstPositiveLabel;

  public MapSortedSessionsIntoLookbackWindows(
      Instant startTime, Instant endTime,
      Duration lookbackGapDuration,
      Duration windowDuration,
      Duration minimumLookaheadDuration, Duration maximumLookaheadDuration,
      boolean stopOnFirstPositiveLabel) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.lookbackGapDuration = lookbackGapDuration;
    this.windowDuration = windowDuration;
    this.minimumLookaheadDuration = minimumLookaheadDuration;
    this.maximumLookaheadDuration = maximumLookaheadDuration;
    this.stopOnFirstPositiveLabel = stopOnFirstPositiveLabel;
  }
}

