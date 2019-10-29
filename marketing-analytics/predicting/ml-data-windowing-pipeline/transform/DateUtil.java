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

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.joda.time.Instant;

/**
 * Utility class for parsing command line date strings into Instants.
 */
final class DateUtil {
  private DateUtil() {
  }

  // Returns the given dateString in dd/MM/yyyy format as an Instant.
  private static Instant parseDateStringToInstantOrDie(String dateString) {
    try {
      return new Instant(
          1000 * LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay()
          * Duration.ofDays(1).getSeconds());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Returns the given stateDateString in dd/MM/yyyy format as an Instant. If the input is null or
  // empty, returns the Epoch Instant. Dies if the input is invalid.
  public static Instant parseStartDateStringToInstant(String stateDateString) {
    Instant startInstant = new Instant(0);
    if (stateDateString != null && !stateDateString.isEmpty()) {
      startInstant = parseDateStringToInstantOrDie(stateDateString);
    }
    return startInstant;
  }

  // Returns the given endDateString in dd/MM/yyyy format as an Instant. If the input is null or
  // empty, returns the maximum possible Instant. Dies if the input is invalid.
  public static Instant parseEndDateStringToInstant(String endDateString) {
    Instant endInstant = new Instant(Long.MAX_VALUE);
    if (endDateString != null  && !endDateString.isEmpty()) {
      endInstant = parseDateStringToInstantOrDie(endDateString);
    }
    return endInstant;
  }
}
