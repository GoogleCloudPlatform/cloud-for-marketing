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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 *  For each Session, outputs one LookbackWindow with the given Session as the last in the window.
 */
public class MapSortedSessionsIntoSessionLookbackWindows extends
    MapSortedSessionsIntoLookbackWindows {

  public MapSortedSessionsIntoSessionLookbackWindows(
      ValueProvider<String> startTime,
      ValueProvider<String> endTime,
      ValueProvider<Long> lookbackGapInSeconds,
      ValueProvider<Long> windowTimeInSeconds,
      ValueProvider<Long> minimumLookaheadTimeInSeconds,
      ValueProvider<Long> maximumLookaheadTimeInSeconds,
      ValueProvider<Boolean> stopOnFirstPositiveLabel) {
    super(startTime,
          endTime,
          lookbackGapInSeconds,
          windowTimeInSeconds,
          minimumLookaheadTimeInSeconds,
          maximumLookaheadTimeInSeconds,
          stopOnFirstPositiveLabel);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Instant startTime = DateUtil.parseStartDateStringToInstant(startTimeProvider.get());
    Instant endTime = DateUtil.parseEndDateStringToInstant(endTimeProvider.get());
    Duration lookbackGapDuration =
        Duration.standardSeconds(lookbackGapInSecondsProvider.get());
    Duration windowDuration = Duration.standardSeconds(windowTimeInSecondsProvider.get());
    Duration minimumLookaheadDuration =
        Duration.standardSeconds(minimumLookaheadTimeInSecondsProvider.get());
    Duration maximumLookaheadDuration =
        Duration.standardSeconds(maximumLookaheadTimeInSecondsProvider.get());
    boolean stopOnFirstPositiveLabel = stopOnFirstPositiveLabelProvider.get();

    KV<String, List<Session>> kv = context.element();
    String userId = kv.getKey();
    ArrayList<Session> sessions = new ArrayList<>(kv.getValue());
    if (sessions.isEmpty()) {
      return;
    }
    ArrayList<Instant> positiveLabelTimes =
        SortedSessionsUtil.getPositiveLabelTimes(sessions, startTime, endTime);

    /*
     * For each Session, outputs one LookbackWindow with the given Session as the last in the
     * window. Note that lastSessionIndex iterates over all the Sessions, while firstSessionIndex is
     * the index of the first Session in the current LookbackWindow defined by lastSessionIndex.
     * As lastSessionIndex increases, the time gap between lastSessionIndex and firstSessionIndex
     * may exceed the windowDuration (e.g. 30d). In this case, increment firstSessionIndex until it
     * falls within the current LookbackWindow.
     */
    int firstSessionIndex = 0;
    for (int lastSessionIndex = 0; lastSessionIndex < sessions.size(); lastSessionIndex++) {
      Session lastSession = sessions.get(lastSessionIndex);
      if (!lastSession.getLastHitTime().isBefore(endTime)) {
        break;
      }
      Session firstSession = sessions.get(firstSessionIndex);
      while (lastSession.getLastHitTime()
                 .minus(lookbackGapDuration).minus(windowDuration)
                 .isAfter(firstSession.getVisitStartTime()))  {
        firstSessionIndex++;
        firstSession = sessions.get(firstSessionIndex);
      }
      LookbackWindow window = new LookbackWindow();
      window.setUserId(userId);
      window.setFirstActivityTime(sessions.get(0).getVisitStartTime());
      window.setStartTime(lastSession.getLastHitTime().minus(lookbackGapDuration).minus(windowDuration));
      window.setEndTime(window.getStartTime().plus(windowDuration));
      window.setEffectiveDate(lastSession.getLastHitTime());
      for (int i = firstSessionIndex; i <= lastSessionIndex; i++) {
        Session session = sessions.get(i);
        if (!session.getVisitStartTime().isBefore(window.getStartTime())
            && !session.getLastHitTime().isAfter(window.getEndTime())) {
          window.addSession(session);
        }
      }
      window.setPredictionLabel(SortedSessionsUtil.hasLabelInInterval(
          positiveLabelTimes,
          minimumLookaheadDuration, maximumLookaheadDuration,
          window.getEndTime()));
      context.output(window);
      if (stopOnFirstPositiveLabel && window.getPredictionLabel()) {
        return;
      }
    }
  }
}
