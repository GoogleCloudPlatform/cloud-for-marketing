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

import com.google.api.services.bigquery.model.TableRow;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Fact;
import com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model.Session;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Converts BigQuery TableRows from Google Analytics to Sessions.
 */
public class MapGATableRowToSession extends DoFn<TableRow, Session> {

  public static final String CUSTOM_DIMENSIONS = "customDimensions";
  public static final String CUSTOM_DIMENSION_VALUE = "value";
  public static final String CUSTOM_DIMENSION_INDEX = "index";
  private final ValueProvider<String> factsToExtractProvider;
  private HashSet<String> factsToExtract;
  // Fact name of the label that we are trying to predict.
  private final ValueProvider<String> labelFactNameProvider;
  private String labelFactName;
  // Positive values of the label we are trying to predict. All other labels for the target fact
  // are considered negative.
  private final ValueProvider<String> positiveLabelFactValuesProvider;
  private HashSet<String> positiveLabelFactValues;

  public MapGATableRowToSession(ValueProvider<String> factsToExtractProvider,
                                ValueProvider<String> labelFactNameProvider,
                                ValueProvider<String> positiveLabelFactValuesProvider) {
    this.factsToExtractProvider = factsToExtractProvider;
    this.labelFactNameProvider = labelFactNameProvider;
    this.positiveLabelFactValuesProvider = positiveLabelFactValuesProvider;
  }

  // Returns given url with the trailing forward slash removed if present and otherwise non-empty.
  private static String normalizeURL(String url) {
    if (url.length() <= 1) {
      return url;
    }
    return url.replaceAll("/$", "");
  }

  // Labels the given factName and factValue if positive, and also adds it to the given session.
  // If a factsToExtract filter is given and the factName is not in the filter, it is not added.
  private void addFactToSession(String factName, String factValue, Instant time, Session session) {
    if (!factsToExtractProvider.get().isEmpty() && !factsToExtract.contains(factName)) {
      return;
    }
    boolean label = factName.equals(labelFactName) && positiveLabelFactValues.contains(factValue);
    session.addFact(
        new Fact(session.getId(), session.getUserId(), time, factName, factValue, label));
  }

  public void addFactsToSession(
      Session session, Instant time, String factNamePrefix, Object object) {
    if (object instanceof TableRow) {
      TableRow tablerow = (TableRow) object;
      Instant childTime = time;
      if (factNamePrefix.equals("hits")) {
        childTime = childTime.plus(
            Duration.millis(Long.parseLong(tablerow.get("time").toString()) / 1000));
        DateTime dateTime = childTime.toDateTime();
        addFactToSession(
            "dayOfWeekOfHit", String.valueOf(dateTime.getDayOfWeek()), childTime, session);
        addFactToSession("hourOfHit", String.valueOf(dateTime.getHourOfDay()), childTime, session);
      }

      if (factNamePrefix.contains(CUSTOM_DIMENSIONS)) {
        addFactsToSession(
            session,
            childTime,
            factNamePrefix + ".index." + tablerow.get(CUSTOM_DIMENSION_INDEX),
            tablerow.get(CUSTOM_DIMENSION_VALUE));
        return;
      }

      for (Map.Entry<String, Object> entry : tablerow.entrySet()) {
        String childFactNamePrefix = factNamePrefix;
        if (!factNamePrefix.isEmpty()) {
          childFactNamePrefix += ".";
        }
        childFactNamePrefix += entry.getKey();
        if (entry.getValue() instanceof List) {
          for (Object childObject : (List) entry.getValue()) {
            addFactsToSession(session, childTime, childFactNamePrefix, childObject);
          }
        } else {
          addFactsToSession(session, childTime, childFactNamePrefix, entry.getValue());
        }
      }
    } else {
      String factValue = object.toString();
      if (factNamePrefix.contains("Path")) {
        factValue = normalizeURL(factValue);
      }
      addFactToSession(factNamePrefix, factValue, time, session);
    }
  }

  void addDerivedTimeFactsToSession(Session session) {
    Instant startTime = session.getVisitStartTime();
    DateTime dateTime = startTime.toDateTime();
    int hourOfDay = dateTime.getHourOfDay();
    String timeOfDay;
    if (hourOfDay >= 6 && hourOfDay <= 11) {
      timeOfDay = "morning";
    } else if (hourOfDay >= 12 && hourOfDay <= 16) {
      timeOfDay = "afternoon";
    } else if (hourOfDay >= 17 && hourOfDay <= 22) {
      timeOfDay = "evening";
    } else {
      timeOfDay = "night";
    }
    addFactToSession("visitStartTimeOfDay", timeOfDay, startTime, session);
    addFactToSession("visitStartDayOfWeek", dateTime.dayOfWeek().getAsText(), startTime, session);
  }

  // Converts BigQuery TableRows from Google Analytics to Sessions.
  @ProcessElement
  public void processElement(ProcessContext context) {
    factsToExtract = new HashSet<String>(
        Arrays.asList(factsToExtractProvider.get().split("\\s*,\\s*")));
    factsToExtract.add("fullVisitorId");
    factsToExtract.add("visitId");
    factsToExtract.add("visitStartTime");
    labelFactName = labelFactNameProvider.get();
    positiveLabelFactValues =
        new HashSet<>(Arrays.asList(positiveLabelFactValuesProvider.get().split(",")));
    TableRow tablerow = context.element();
     Session session = new Session();
    session.setId(String.format("%s/%s", tablerow.get("fullVisitorId"), tablerow.get("visitId")));
    session.setUserId(tablerow.get("fullVisitorId").toString());
    session.setVisitStartTime(
        new Instant(1000 * Long.parseLong(tablerow.get("visitStartTime").toString())));
    addDerivedTimeFactsToSession(session);
    addFactsToSession(session, session.getVisitStartTime(), "", tablerow);
    context.output(session);
  }
}
