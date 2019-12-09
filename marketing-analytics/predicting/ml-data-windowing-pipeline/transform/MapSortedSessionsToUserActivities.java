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
 * Outputs snapshots that summarizes the user's activity up to the snapshot date.
 * The first snapshot is at snapshotStartDate. Each subsequent snapshot date occurs
 * slideTimeInSeconds after the previous one. The last possible snapshot occurs at snapshotEndDate.
 */
public class MapSortedSessionsToUserActivities extends DoFn<
    KV<String, List<Session>>, UserActivity> {

  protected ValueProvider<String> snapshotStartDateProvider;
  protected ValueProvider<String> snapshotEndDateProvider;
  protected ValueProvider<Long> slideTimeInSecondsProvider;
  protected ValueProvider<Long> minimumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Long> maximumLookaheadTimeInSecondsProvider;
  protected ValueProvider<Boolean> stopOnFirstPositiveLabelProvider;

  public MapSortedSessionsToUserActivities(
      ValueProvider<String> snapshotStartDate,
      ValueProvider<String> snapshotEndDate,
      ValueProvider<Long> slideTimeInSeconds,
      ValueProvider<Long> minimumLookaheadTimeInSeconds,
      ValueProvider<Long> maximumLookaheadTimeInSeconds,
      ValueProvider<Boolean> stopOnFirstPositiveLabel) {
    snapshotStartDateProvider = snapshotStartDate;
    snapshotEndDateProvider = snapshotEndDate;
    slideTimeInSecondsProvider = slideTimeInSeconds;
    minimumLookaheadTimeInSecondsProvider = minimumLookaheadTimeInSeconds;
    maximumLookaheadTimeInSecondsProvider = maximumLookaheadTimeInSeconds;
    stopOnFirstPositiveLabelProvider = stopOnFirstPositiveLabel;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Instant snapshotStartDate = DateUtil.parseStartDateStringToInstant(
        snapshotStartDateProvider.get());
    Instant snapshotEndDate = DateUtil.parseEndDateStringToInstant(
        snapshotEndDateProvider.get());
    Duration slideDuration = Duration.standardSeconds(slideTimeInSecondsProvider.get());
    Duration minimumLookaheadDuration =
        Duration.standardSeconds(minimumLookaheadTimeInSecondsProvider.get());
    Duration maximumLookaheadDuration =
        Duration.standardSeconds(maximumLookaheadTimeInSecondsProvider.get());
    Boolean stopOnFirstPositiveLabel = stopOnFirstPositiveLabelProvider.get();
    Instant firstPositiveLabelInstant = null;

    KV<String, List<Session>> kv = context.element();
    String userId = kv.getKey();
    ArrayList<Session> sessions = new ArrayList<>(kv.getValue());
    if (sessions.isEmpty()) {
      return;
    }
    ArrayList<Instant> positiveLabelTimes = SortedSessionsUtil.getPositiveLabelTimes(
        sessions, snapshotStartDate, snapshotEndDate);

    int sessionIndex = 0;
    UserActivity userActivity = new UserActivity();
    userActivity.setUserId(userId);
    for (Instant snapshotTime = snapshotStartDate;
         !snapshotTime.isAfter(snapshotEndDate);
         snapshotTime = snapshotTime.plus(slideDuration)) {
      if (stopOnFirstPositiveLabel
          && firstPositiveLabelInstant != null
          && firstPositiveLabelInstant.isBefore(snapshotTime.plus(minimumLookaheadDuration))) {
        break;
      }
      userActivity.setSnapshotTime(snapshotTime);
      userActivity.setDurationSinceStartDate(new Duration(snapshotStartDate, snapshotTime));
      Instant positiveLabelInstant = SortedSessionsUtil.getFirstInstantInInterval(
          positiveLabelTimes,
          snapshotTime.plus(minimumLookaheadDuration),
          snapshotTime.plus(maximumLookaheadDuration));
      userActivity.setHasPositiveLabel(positiveLabelInstant != null);
      if (firstPositiveLabelInstant == null) {
        firstPositiveLabelInstant = positiveLabelInstant;
      }
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
      // Don't output a userActivity if there has been no activity, even if there is a positive
      // label in the prediction window.
      if (userActivity.getDurationSinceFirstActivity() == null) {
        continue;
      }
      context.output(userActivity);
    }
  }
}
