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

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.feature.accumulator;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Factory class to create FeatureAccumulator instances. */
public class FeatureAccumulatorFactory implements Serializable {

  private static final long serialVersionUID = -8334117609194707958L;

  public static final String NUMERIC = "NUMERIC";
  public static final String STRING = "STRING";
  public static final Splitter OPTION_COLUMN_SPLITTER =
      Splitter.on(':').trimResults().omitEmptyStrings().trimResults(CharMatcher.anyOf("[]"));
  public static final Splitter OPTION_COLUMN_VALUE_LIST_SPLITTER =
      Splitter.on(',').trimResults().omitEmptyStrings();
  public static final CharMatcher ALPHA_NUMERIC_MATCHER =
      CharMatcher.inRange('0', '9')
          .or(CharMatcher.inRange('a', 'z'))
          .or(CharMatcher.inRange('A', 'Z'));

  // Pattern: factName:[comma-separated fact values]:[default value]
  // Examples:
  // operatingSystem:[Macintosh,Android,Windows,iOS]:[Others]
  // operatingSystem:[Macintosh,Android,Windows,iOS]:[]
  // hits:[]:[]
  private static final Pattern FACT_MATCHER =
      Pattern.compile(
          "([a-zA-Z0-9| |_|\\-|\\+|\\(|\\)|\\.|\\[|\\]|\\/]*):\\[(([a-zA-Z0-9|"
              + " |_|\\-|\\+|\\(|\\)|\\.|\\[|\\]|\\/]+,?)*)\\]:\\[([a-zA-Z0-9]*)]");
  public static final String UNDERSCORE = "_";
  public static final int BQ_COLUMN_NAME_MAX_LENGTH = 128;

  /** Creates {@link FeatureAccumulator} based on an {@link AccumulatorOptions}. */
  public FeatureAccumulator createAccumulator(AccumulatorOptions opt) {
    switch (AccumulatorType.valueOf(opt.type())) {
      case PROPORTION:
        return new ProportionValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case MOST_FREQUENT:
        return new MostFrequentValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case SUM:
        return new SumValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case AVERAGE:
        return new AverageValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case COUNT:
        return new CountValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case RECENT:
        return new RecentValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
      case AVERAGE_BY_TENURE:
        return new AverageByTenureValueFeatureAccumulator(
            opt.column(), opt.valueToFeatureName(), opt.defaultValue());
    }
    throw new IllegalArgumentException("Invalid accumulator type: " + opt.type());
  }

  public Multimap<String, AccumulatorOptions> createAccumulatorOptions(
      AccumulatorType accumulatorType, String options) {
    Matcher m = FACT_MATCHER.matcher(options);
    Multimap<String, AccumulatorOptions> result = ArrayListMultimap.create();
    while (m.find()) {
      String factGroup = m.group();
      List<String> factParam = OPTION_COLUMN_SPLITTER.splitToList(factGroup);
      if (!factParam.isEmpty()) {
        String factName = factParam.get(0);

        // Extract the fact values if accumulator requires value list.
        String factValues = getOptionParameter(factParam, 1);
        if (accumulatorType.isValueListRequired() && Strings.isNullOrEmpty(factValues)) {
          throw new IllegalArgumentException(
              "Required value list for accumulator type : " + accumulatorType);
          }

        // Extract the fact default value.
        String defaultValue = getOptionParameter(factParam, 2);

        result.put(
            factName,
            createAccumulatorOptions(factValues, accumulatorType, factName, defaultValue));
      }
    }
    return result;
  }

  private AccumulatorOptions createAccumulatorOptions(
      String factValues, AccumulatorType accumulatorType, String factName, String defaultValue) {
    List<String> values = new ArrayList<>();
    if (!factValues.isEmpty()) {
      values.addAll(OPTION_COLUMN_VALUE_LIST_SPLITTER.splitToList(factValues));
    }
    return AccumulatorOptions.builder()
        .setColumn(factName)
        .setValueToFeatureName(
            createValueToFeatureNameMap(accumulatorType, values, factName, defaultValue))
        .setType(accumulatorType.toString())
        .setSchemaType(getSchemaType(accumulatorType))
        .setDefaultValue(defaultValue)
        .build();
  }

  private static String getOptionParameter(List<String> factParam, int index) {
    String paramValue = "";
    if (factParam.size() > index) {
      paramValue = factParam.get(index);
    }
    return paramValue;
  }

  protected ImmutableMap<String, String> createValueToFeatureNameMap(
      AccumulatorType accumulatorType, List<String> values, String factName, String defaultValue) {
    String prefix = accumulatorType + UNDERSCORE + encodeValueName(factName);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    // Add default value to the list of values being considered.
    if (!Strings.isNullOrEmpty(defaultValue) && !values.contains(defaultValue)) {
      values.add(defaultValue);
    }

    for (String value : values) {
      if (accumulatorType.isSingleOutput()) {
        builder.put(value, truncateColumnNameIfNeeded(prefix));
      } else {
        builder.put(
            value, truncateColumnNameIfNeeded(prefix + UNDERSCORE + encodeValueName(value)));
      }
    }

    // If there are no value list, add the fact name.
    if (!accumulatorType.isValueListRequired() && values.isEmpty()) {
      builder.put(factName, truncateColumnNameIfNeeded(prefix));
    }

    return builder.build();
  }

  protected String getSchemaType(AccumulatorType accumulatorType) {
    switch (accumulatorType) {
      case PROPORTION:
      case SUM:
      case COUNT:
      case AVERAGE_BY_TENURE:
      case AVERAGE:
        return NUMERIC;
      case MOST_FREQUENT:
      case RECENT:
        return STRING;
    }
    return STRING;
  }

  private static String encodeValueName(String s) {
    StringBuilder encodedValueName = new StringBuilder();
    for (Character c : s.toCharArray()) {
      if (!ALPHA_NUMERIC_MATCHER.matches(c)) {
        int i = StandardCharsets.UTF_8.encode(c.toString()).get();
        encodedValueName.append('_').append(i);
      } else {
        encodedValueName.append(c);
      }
    }
    return encodedValueName.toString();
  }

  private static String truncateColumnNameIfNeeded(String featureColumnName) {
    return featureColumnName.length() > BQ_COLUMN_NAME_MAX_LENGTH
        ? featureColumnName.substring(0, BQ_COLUMN_NAME_MAX_LENGTH)
        : featureColumnName;
  }
}
