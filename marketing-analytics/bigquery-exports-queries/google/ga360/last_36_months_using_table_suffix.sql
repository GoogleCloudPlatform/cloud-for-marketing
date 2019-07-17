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
-- This script aggregates the last 36 months worth of data (visits, pageviews, transactions, revenue) into one table using _TABLE_SUFFIX

SELECT
date,
SUM(totals.visits) AS visits,
SUM(totals.pageviews) AS pageviews,
SUM(totals.transactions) AS transactions,
SUM(totals.transactionRevenue)/1000000 AS revenue
FROM
(TABLE_DATE_RANGE([bigquery-public-data.google_analytics_sample.ga_sessions_],
DATE_ADD(CURRENT_TIMESTAMP(), -36, 'MONTH'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')))
GROUP BY
date
ORDER BY
date ASC
