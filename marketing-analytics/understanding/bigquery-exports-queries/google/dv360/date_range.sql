-- Copyright 2018 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- This script obtains the partition and event date ranges for each of the view,
-- click, and activity tables. It is meant to give you a range of dates that you
-- can use in the other queries to process only limited data.
SELECT
  "VIEW" AS Event_Type,
  FORMAT_DATE("%F", MIN(_DATA_DATE)) AS Start_Date_Partition,
  FORMAT_DATE("%F", MAX(_DATA_DATE)) AS End_Date_Partition,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MIN(Event_Time))) AS Start_Date_Event,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MAX(Event_Time))) AS End_Date_Event
FROM
  `project.dataset.impression`
UNION ALL
SELECT
  "CLICK" AS Event_Type,
  FORMAT_DATE("%F", MIN(_DATA_DATE)) AS Start_Date_Partition,
  FORMAT_DATE("%F", MAX(_DATA_DATE)) AS End_Date_Partition,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MIN(Event_Time))) AS Start_Date_Event,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MAX(Event_Time))) AS End_Date_Event
FROM
  `project.dataset.click`
UNION ALL
SELECT
  "CONVERSION" AS Event_Type,
  FORMAT_DATE("%F", MIN(_DATA_DATE)) AS Start_Date_Partition,
  FORMAT_DATE("%F", MAX(_DATA_DATE)) AS End_Date_Partition,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MIN(Event_Time))) AS Start_Date_Event,
  FORMAT_TIMESTAMP("%F %T", TIMESTAMP_MICROS(MAX(Event_Time))) AS End_Date_Event
FROM
 `project.dataset.activity`