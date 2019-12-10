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
      ValueProvider<String> snapshotStartDate,
      ValueProvider<String> snapshotEndDate,
      ValueProvider<Long> lookbackGapInSeconds,
      ValueProvider<Long> windowTimeInSeconds,
      ValueProvider<Long> minimumLookaheadTimeInSeconds,
      ValueProvider<Long> maximumLookaheadTimeInSeconds,
      ValueProvider<Boolean> stopOnFirstPositiveLabel) {
    super(snapshotStartDate,
          snapshotEndDate,
          lookbackGapInSeconds,
          windowTimeInSeconds,
          minimumLookaheadTimeInSeconds,
          maximumLookaheadTimeInSeconds,
          stopOnFirstPositiveLabel);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Instant snapshotStartDate = DateUtil.parseStartDateStringToInstant(
        snapshotStartDateProvider.get());
    Instant snapshotEndDate = DateUtil.parseEndDateStringToInstant(snapshotEndDateProvider.get());
    Duration lookbackGapDuration =
        Duration.standardSeconds(lookbackGapInSecondsProvider.get());
    Duration windowDuration = Duration.standardSeconds(windowTimeInSecondsProvider.get());
    Duration minimumLookaheadDuration =
        Duration.standardSeconds(minimumLookaheadTimeInSecondsProvider.get());
    Duration maximumLookaheadDuration =
        Duration.standardSeconds(maximumLookaheadTimeInSecondsProvider.get());
    boolean stopOnFirstPositiveLabel = stopOnFirstPositiveLabelProvider.get();
    Instant firstPositiveLabelInstant = null;

    KV<String, List<Session>> kv = context.element();
    String userId = kv.getKey();
    ArrayList<Session> sessions = new ArrayList<>(kv.getValue());
    if (sessions.isEmpty()) {
      return;
    }
    ArrayList<Instant> positiveLabelTimes =
        SortedSessionsUtil.getPositiveLabelTimes(sessions, snapshotStartDate, snapshotEndDate);

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
      Instant effectiveDate = lastSession.getLastHitTime();
      if (stopOnFirstPositiveLabel
          && firstPositiveLabelInstant != null
          && firstPositiveLabelInstant.isBefore(effectiveDate.plus(minimumLookaheadDuration))) {
        break;
      }
      if (effectiveDate.isBefore(snapshotStartDate)) {
        continue;
      }
      if (effectiveDate.isAfter(snapshotEndDate)) {
        break;
      }
      Session firstSession = sessions.get(firstSessionIndex);
      while (effectiveDate
                 .minus(lookbackGapDuration).minus(windowDuration)
                 .isAfter(firstSession.getVisitStartTime()))  {
        firstSessionIndex++;
        firstSession = sessions.get(firstSessionIndex);
      }
      LookbackWindow window = new LookbackWindow();
      window.setUserId(userId);
      window.setFirstActivityTime(sessions.get(0).getVisitStartTime());
      window.setStartTime(effectiveDate.minus(lookbackGapDuration).minus(windowDuration));
      window.setEndTime(window.getStartTime().plus(windowDuration));
      window.setEffectiveDate(effectiveDate);
      for (int i = firstSessionIndex; i <= lastSessionIndex; i++) {
        Session session = sessions.get(i);
        if (!session.getVisitStartTime().isBefore(window.getStartTime())
            && !session.getLastHitTime().isAfter(window.getEndTime())) {
          window.addSession(session);
        }
      }
      Instant positiveLabelInstant = SortedSessionsUtil.getFirstInstantInInterval(
          positiveLabelTimes,
          effectiveDate.plus(minimumLookaheadDuration),
          effectiveDate.plus(maximumLookaheadDuration));
      if (firstPositiveLabelInstant == null) {
        firstPositiveLabelInstant = positiveLabelInstant;
      }
      window.setPredictionLabel(positiveLabelInstant != null);
      context.output(window);
    }
  }
}
