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
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.UserActivity;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * For each day between the given startTime and endTime, outputs summary UserActivity of the user's
 * Sessions up to this point.
 */
public class MapSortedSessionsToUserActivities extends DoFn<
    KV<String, List<Session>>, UserActivity> {

  protected ValueProvider<String> startTimeProvider;
  protected ValueProvider<String> endTimeProvider;
  protected ValueProvider<Long> slideTimeInSecondsProvider;
  protected ValueProvider<Long> minimumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Long> maximumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Boolean> stopOnFirstPositiveLabelProvider;

  public MapSortedSessionsToUserActivities(
      ValueProvider<String> startTime,
      ValueProvider<String> endTime,
      ValueProvider<Long> slideTimeInSeconds,
      ValueProvider<Long> minimumLookaheadTimeInSeconds,
      ValueProvider<Long> maximumLookaheadTimeInSeconds,
      ValueProvider<Boolean> stopOnFirstPositiveLabel) {
    startTimeProvider = startTime;
    endTimeProvider = endTime;
    slideTimeInSecondsProvider = slideTimeInSeconds;
    minimumLookaheadTimeInSecondsProvider = minimumLookaheadTimeInSeconds;
    maximumLookaheadTimeInSecondsProvider = maximumLookaheadTimeInSeconds;
    stopOnFirstPositiveLabelProvider = stopOnFirstPositiveLabel;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Instant startTime = DateUtil.parseStartDateStringToInstant(startTimeProvider.get());
    Instant endTime = DateUtil.parseEndDateStringToInstant(endTimeProvider.get());
    Duration slideDuration = Duration.standardSeconds(slideTimeInSecondsProvider.get());
    Duration minimumLookaheadDuration =
        Duration.standardSeconds(minimumLookaheadTimeInSecondsProvider.get());
    Duration maximumLookaheadDuration =
        Duration.standardSeconds(maximumLookaheadTimeInSecondsProvider.get());
    Boolean stopOnFirstPositiveLabel = stopOnFirstPositiveLabelProvider.get();

    KV<String, List<Session>> kv = context.element();
    String userId = kv.getKey();
    ArrayList<Session> sessions = new ArrayList<>(kv.getValue());
    if (sessions.isEmpty()) {
      return;
    }
    ArrayList<Instant> positiveLabelTimes = SortedSessionsUtil.getPositiveLabelTimes(
        sessions, startTime, endTime);

    int sessionIndex = 0;
    UserActivity userActivity = new UserActivity();
    userActivity.setUserId(userId);
    for (Instant snapshotTime = startTime;
         snapshotTime.isBefore(endTime);
         snapshotTime = snapshotTime.plus(slideDuration)) {
      userActivity.setSnapshotTime(snapshotTime);
      userActivity.setDurationSinceStartDate(new Duration(startTime, snapshotTime));
      userActivity.setHasPositiveLabel(SortedSessionsUtil.hasLabelInInterval(
          positiveLabelTimes, minimumLookaheadDuration, maximumLookaheadDuration, snapshotTime));
      if (userActivity.getDurationSinceFirstActivity() != null) {
        userActivity.setDurationSinceFirstActivity(
            userActivity.getDurationSinceFirstActivity().plus(slideDuration));
      }
      if (userActivity.getDurationSinceLatestActivity() != null) {
        userActivity.setDurationSinceLatestActivity(
            userActivity.getDurationSinceLatestActivity().plus(slideDuration));
      }
      // Update the userActivity with Sessions that finish before the snapshotTime.
      for (; sessionIndex < sessions.size(); sessionIndex++) {
        Session session = sessions.get(sessionIndex);
        if (session.getLastHitTime().isAfter(snapshotTime)) {
          break;
        }
        Duration durationSinceSessionEnd = new Duration(session.getLastHitTime(), snapshotTime);
        userActivity.setDurationSinceLatestActivity(durationSinceSessionEnd);
        if (userActivity.getDurationSinceFirstActivity() == null) {
          userActivity.setDurationSinceFirstActivity(durationSinceSessionEnd);
        }
      }
      context.output(userActivity);
      if (stopOnFirstPositiveLabel && userActivity.getHasPositiveLabel()) {
          break;
      }
    }
  }
}
