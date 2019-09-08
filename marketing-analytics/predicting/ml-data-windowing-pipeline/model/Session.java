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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

/**
 * A Session is a collection of Facts for a userId that all occur at in one continuous time period.
 */
@DefaultCoder(AvroCoder.class)
public class Session {
  // Unique ID for this Session
  private String id;
  // Unique ID of the User.
  private String userId;
  // Start time of the Session.
  private Instant visitStartTime;
  // Time of the last hit in the Session.
  private Instant lastHitTime;
  // Collection of facts that occur during the Session.
  private final ArrayList<Fact> facts;

  public Session() {
    id = null;
    userId = null;
    visitStartTime = null;
    lastHitTime = null;
    facts = new ArrayList<>();
  }

  public String getId() {
    return id;
  }

  public String getUserId() {
    return userId;
  }

  public Instant getVisitStartTime() {
    return visitStartTime;
  }

  public Instant getLastHitTime() {
    return lastHitTime;
  }

  public ArrayList<Fact> getFacts() {
    return facts;
  }

  // Returns true if any Fact has a positive label and false otherwise.
  public boolean hasPositiveLabel() {
    for (Fact fact : facts) {
      if (fact.getHasPositiveLabel() != null && fact.getHasPositiveLabel()) {
        return true;
      }
    }
    return false;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setVisitStartTime(Instant visitStartTime) {
    this.visitStartTime = visitStartTime;
  }

  public void setLastHitTime(Instant lastHitTime) {
    this.lastHitTime = lastHitTime;
  }

  public void addFact(Fact fact) {
    facts.add(fact);
    if (lastHitTime == null || fact.getTime().isAfter(lastHitTime)) {
      lastHitTime = fact.getTime();
    }
  }

  // Creates and adds a new Fact with the given name, value and label for this Session's userId and
  // visitStartTime.
  public void addFact(String name, String value, boolean label) {
    addFact(new Fact(id, userId, visitStartTime, name, value, label));
  }
}
