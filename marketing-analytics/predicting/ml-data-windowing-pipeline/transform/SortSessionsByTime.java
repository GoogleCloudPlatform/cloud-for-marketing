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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Given a collection of Sessions for a user, outputs the Sessions sorted by time.
 */
public class SortSessionsByTime extends DoFn<
    KV<String, Iterable<Session>>, KV<String, List<Session>>> {

  public SortSessionsByTime() {
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    KV<String, Iterable<Session>> kv = context.element();
    ArrayList<Session> sessions = new ArrayList<>();
    for (Session session : kv.getValue()) {
      sessions.add(session);
    }
    if (sessions.isEmpty()) {
      return;
    }
    Collections.sort(sessions, new Comparator<Session>() {
      @Override
      public int compare(Session lhs, Session rhs) {
        int result = lhs.getLastHitTime().compareTo(rhs.getLastHitTime());
        if (result == 0) {
          return lhs.getVisitStartTime().compareTo(rhs.getVisitStartTime());
        }
        return result;
      }
    });
    context.output(KV.of(kv.getKey(), sessions));
  }
}
