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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * UserActivity is a data exploration class for summarizing a given userId's activity from the
 * global start date to a given snapshotTime. If the user has a positive label Fact within the
 * prediction window, then hasPositiveLabel is set to true.
 */
@DefaultCoder(AvroCoder.class)
public class UserActivity {
  // Unique ID of the User.
  private String userId;
  // True if the user has a positive label Fact within the prediction window following this day.
  private boolean hasPositiveLabel;
  // Duration between the global start date until this activty summary.
  @Nullable private Duration durationSinceStartDate;
  // Duration between the userId's first Session until this activity summary.
  // Null if the userId has had no Sessions.
  @Nullable private Duration durationSinceFirstActivity;
  // Duration between the userId's latest Session until this activity summary.
  // Null if the userId has had no Sessions.
  @Nullable private Duration durationSinceLatestActivity;
  // Timestamp of this snapshot.
  private Instant snapshotTime;

  public UserActivity() {
    userId = null;
    hasPositiveLabel = false;
    durationSinceStartDate = null;
    durationSinceFirstActivity = null;
    durationSinceLatestActivity = null;
    snapshotTime = null;
  }

  public String getUserId() {
    return userId;
  }

  public boolean getHasPositiveLabel() {
    return hasPositiveLabel;
  }

  public Duration getDurationSinceStartDate() {
    return durationSinceStartDate;
  }

  public Duration getDurationSinceFirstActivity() {
    return durationSinceFirstActivity;
  }

  public Duration getDurationSinceLatestActivity() {
    return durationSinceLatestActivity;
  }

  public Instant getSnapshotTime() {
    return snapshotTime;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setHasPositiveLabel(boolean hasPositiveLabel) {
    this.hasPositiveLabel = hasPositiveLabel;
  }

  public void setDurationSinceStartDate(Duration durationSinceStartDate) {
    this.durationSinceStartDate = durationSinceStartDate;
  }

  public void setDurationSinceFirstActivity(Duration durationSinceFirstActivity) {
    this.durationSinceFirstActivity = durationSinceFirstActivity;
  }

  public void setDurationSinceLatestActivity(Duration durationSinceLatestActivity) {
    this.durationSinceLatestActivity = durationSinceLatestActivity;
  }

  public void setSnapshotTime(Instant snapshotTime) {
    this.snapshotTime = snapshotTime;
  }
}
