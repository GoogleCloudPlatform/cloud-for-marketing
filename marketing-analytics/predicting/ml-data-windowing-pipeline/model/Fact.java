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
import org.joda.time.Instant;

/**
 * A Fact records a variable name, value and fact timestamp for a userId. A Fact can represent
 * a Google Analytics data point (e.g. user device, geolocation, page hit etc), or CRM data from
 * another source. All Facts belong to a user Session. If the Fact is the fact being predicted
 * (e.g. account signup, purchase, account churn etc), then hasPositiveLabel is set to true.
 */
@DefaultCoder(AvroCoder.class)
public class Fact {
  // Unique ID of the Session this Fact belongs to.
  private  String sessionId;
  // Unique ID of the User.
  private String userId;
  // Time this fact occurred.
  private Instant time;
  // Name of the variable.
  private  String name;
  // Value of the variable.
  private String value;
  // True if this fact represents the fact that is being predicted.
  @Nullable private Boolean hasPositiveLabel;

  public Fact() {
    this(null, null, null, null, null, null);
  }

  public Fact(String sessionId, String userId, Instant time, String name, String value) {
    this(sessionId, userId, time, name, value, null /* hasPositiveLabel */);
  }

  public Fact(
      String sessionId,
      String userId,
      Instant time,
      String name,
      String value,
      Boolean hasPositiveLabel) {
    this.sessionId = sessionId;
    this.userId = userId;
    this.time = time;
    this.name = name;
    this.value = value;
    this.hasPositiveLabel = hasPositiveLabel;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUserId() {
    return userId;
  }

  public Instant getTime() {
    return time;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public Boolean getHasPositiveLabel() {
    return hasPositiveLabel;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setTime(Instant time) {
    this.time = time;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setHasPositiveLabel(Boolean hasPositiveLabel) {
    this.hasPositiveLabel = hasPositiveLabel;
  }
}
