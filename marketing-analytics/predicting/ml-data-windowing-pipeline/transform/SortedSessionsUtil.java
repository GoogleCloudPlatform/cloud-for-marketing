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

import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Session;
import java.util.ArrayList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Utility functions for processing Sessions sorted by time.
 */
public class SortedSessionsUtil {

  private SortedSessionsUtil() {
  }

  // Returns the collection of positive label time instants for the given Sessions between the
  // given start (inclusive) and end instant (not inclusive).
  public static ArrayList<Instant> getPositiveLabelTimes(
      ArrayList<Session> sessions, Instant startTime, Instant endTime) {
    ArrayList<Instant> positiveLabelTimes = new ArrayList<>();
    for (Session session : sessions) {
      if (!session.hasPositiveLabel()) {
        continue;
      }
      if (session.getVisitStartTime().isBefore(startTime)
          || !session.getLastHitTime().isBefore(endTime)) {
        continue;
      }
      positiveLabelTimes.add(session.getVisitStartTime());
    }
    return positiveLabelTimes;
  }

  // Return true if there is a positiveLabelTime in the interval between
  // [time + minimumLookaheadTimeInSeconds, time + maximumLookaheadTimeInSeconds].
  // Assumes positiveInstants in sorted order.
  public static boolean hasLabelInInterval(
      ArrayList<Instant> positiveLabelTimes,
      Duration minimumLookaheadDuration,
      Duration maximumLookaheadDuration,
      Instant time) {
    for (Instant positiveLabelTime : positiveLabelTimes) {
      if (positiveLabelTime.compareTo(time.plus(maximumLookaheadDuration)) > 0) {
        break;
      }
      if (positiveLabelTime.compareTo(time.plus(minimumLookaheadDuration)) >= 0) {
        return true;
      }
    }
    return false;
  }

  // Returns the day offset from Epoch for the given instant.
  public static long getEpochDay(Instant instant) {
    return Duration.millis(instant.getMillis()).getStandardDays();
  }
}
