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

import java.util.ArrayList;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

/**
 * A LookbackWindow stores the collection of Sessions for a given userId that start between the
 * given start and end time inclusive. If the user has a positive label Fact within the prediction
 * window, then Lookbackwindow.hasPositiveLabel is set to true.
 */
@DefaultCoder(AvroCoder.class)
public class LookbackWindow {
  // Unique ID of the User.
  private String userId;
  // Start time of the LookbackWindow.
  private Instant startTime;
  // End time of the LookbackWindow.
  private Instant endTime;
  private Instant effectiveDate;
  // Collection of Sessions that occur during the LookbackWindow.
  private final ArrayList<Session> sessions;
  // True if the user has a positive label Fact within the prediction window
  // following this LookbackWindow.
  @Nullable private Boolean predictionLabel;
  // Time of the first activity for this user, even if it is outside this window.
  @Nullable private Instant firstActivityTime;

  public LookbackWindow() {
    userId = null;
    startTime = null;
    endTime = null;
    effectiveDate = null;
    sessions = new ArrayList<>();
    predictionLabel = null;
    firstActivityTime = null;
  }

  public String getUserId() {
    return userId;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public Instant getEffectiveDate() {
    return effectiveDate;
  }

  public ArrayList<Session> getSessions() {
    return sessions;
  }

  public Boolean getPredictionLabel() {
    return predictionLabel;
  }

  public Instant getFirstActivityTime() {
    return firstActivityTime;
  }

  public boolean isEmpty() {
    return sessions.isEmpty();
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setStartTime(Instant startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(Instant endTime) {
    this.endTime = endTime;
  }

  public void setEffectiveDate(Instant effectiveDate) {
    this.effectiveDate = effectiveDate;
  }

  public void addSession(Session session) {
    sessions.add(session);
  }

  public void setPredictionLabel(Boolean predictionLabel) {
    this.predictionLabel = predictionLabel;
  }

  public void setFirstActivityTime(Instant firstActivityTime) {
    this.firstActivityTime = firstActivityTime;
  }
}
