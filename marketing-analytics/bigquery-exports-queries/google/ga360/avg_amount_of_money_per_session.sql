#standardSQL

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

-- This example is using the Google analytics sample data set
-- This script calculates the average amount of money spent per session

WITH Sessions AS (
  SELECT
    fullVisitorId,
    SUM(totals.visits) AS total_visits_per_user,
    SUM(totals.transactionRevenue) AS total_transactionrevenue_per_user
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
    AND totals.visits > 0
    AND totals.transactions >= 1
    AND totals.transactionRevenue IS NOT NULL
  GROUP BY fullVisitorId
)
SELECT
  (SUM(total_transactionrevenue_per_user/1e6) /
      SUM(total_visits_per_user)) AS avg_revenue_by_user_per_visit
FROM Sessions
