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
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Outputs sliding-window LookbackWindows of Session data for the given user.
 * Each LookbackWindow has a fixed time period called windowTime.
 * The first possible LookbackWindow begins at startTimeInSeconds.
 * The next possible LookbackWindow begins at time startTimeInSeconds + slideTime.
 * Each subsequent possible LookbackWindow occurs at the previous
 * LookbackWindow.startTime + slideTime.
 *
 * Note that we only output a LookbackWindow if it contains at least one Session. Also, if
 * stopOnFirstPositiveLabel is true, then no more LookbackWindows are output once the first one
 * with a positive label is seen.
 */
public class MapSortedSessionsIntoSlidingLookbackWindows extends
    MapSortedSessionsIntoLookbackWindows {
  protected Duration slideDuration;
  public MapSortedSessionsIntoSlidingLookbackWindows(
      Instant startTime, Instant endTime,
      Duration lookbackGapDuration, Duration windowDuration, Duration slideDuration,
      Duration minimumLookaheadDuration, Duration maximumLookaheadDuration,
      boolean stopOnFirstPositiveLabel) {
    super(startTime, endTime, lookbackGapDuration, windowDuration,
        minimumLookaheadDuration, maximumLookaheadDuration,
        stopOnFirstPositiveLabel);
    this.slideDuration = slideDuration;
  }

  @ProcessElement
  // TODO(dabraham): Add tests.
  public void processElement(ProcessContext context) {
    KV<String, List<Session>> kv = context.element();
    String userId = kv.getKey();
    ArrayList<Session> sessions = new ArrayList<>(kv.getValue());
    if (sessions.isEmpty()) {
      return;
    }
    ArrayList<Instant> positiveLabelTimes =
        SortedSessionsUtil.getPositiveLabelTimes(sessions, startTime, endTime);

    // Iterate over all possible LookbackWindows from startTime, moving forwards each time
    // by slideDuration.
    // Note: For simplicity, the code always advances by one slideDuration at a time, instead of
    // jumping over large gaps in Session times. This has minimal impact on runtime compared to
    // the overall IO bottlenecks.
    int sessionStartIndex = 0;
    for (Instant windowStartTime = startTime;
         windowStartTime.plus(windowDuration).plus(lookbackGapDuration).isBefore(endTime);
         windowStartTime = windowStartTime.plus(slideDuration)) {
      // Find the first Session to start in the LookbackWindow.
      while (sessionStartIndex < sessions.size()
             && windowStartTime.isAfter(sessions.get(sessionStartIndex).getVisitStartTime())) {
        sessionStartIndex++;
      }
      // Return early if all Sessions occur before windowStartTime.
      if (sessionStartIndex >= sessions.size()) {
        return;
      }
      // Skip empty LookbackWindows until the first user activity is within the window duration plus
      // lookbackGapDuration.
      if (sessionStartIndex == 0 && sessions.get(sessionStartIndex).getLastHitTime().isAfter(
              windowStartTime.plus(windowDuration).plus(lookbackGapDuration))) {
        continue;
      }
      // Construct a LookbackWindow.
      LookbackWindow window = new LookbackWindow();
      window.setFirstActivityTime(sessions.get(0).getVisitStartTime());
      window.setUserId(userId);
      window.setStartTime(windowStartTime);
      window.setEndTime(windowStartTime.plus(windowDuration));
      window.setEffectiveDate(window.getEndTime().plus(lookbackGapDuration));
      // Add Sessions to the LookbackWindow if they occur within the LookbackWindow time interval.
      for (int i = sessionStartIndex; i < sessions.size(); i++) {
        Session session = sessions.get(i);
        if (!session.getVisitStartTime().isBefore(window.getStartTime())
            && !session.getLastHitTime().isAfter(window.getEndTime())) {
          window.addSession(session);
        }
      }
      window.setPredictionLabel(SortedSessionsUtil.hasLabelInInterval(
          positiveLabelTimes,
          minimumLookaheadDuration,
          maximumLookaheadDuration,
          window.getEndTime()));
      context.output(window);
      if (stopOnFirstPositiveLabel && window.getPredictionLabel()) {
        return;
      }
    }
  }
}

