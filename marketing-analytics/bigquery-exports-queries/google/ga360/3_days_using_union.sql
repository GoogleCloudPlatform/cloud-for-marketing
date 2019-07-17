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

-- This is using the Google analytics sample data set
-- This script aggregates 3 days worth of data (visits, pageviews, transactions, revenue) into one table using a UNION ALL

WITH ga_tables AS (
SELECT
date,
SUM(totals.visits) AS visits,
SUM(totals.pageviews) AS pageviews,
SUM(totals.transactions) AS transactions,
SUM(totals.transactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`
GROUP BY date

UNION ALL

SELECT
date,
SUM(totals.visits) AS visits,
SUM(totals.pageviews) AS pageviews,
SUM(totals.transactions) AS transactions,
SUM(totals.transactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160802`
GROUP BY date

UNION ALL

SELECT
date,
SUM(totals.visits) AS visits,
SUM(totals.pageviews) AS pageviews,
SUM(totals.transactions) AS transactions,
SUM(totals.transactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160803`
GROUP BY date

)
SELECT
date,
visits,
pageviews,
transactions,
revenue,
FROM ga_tables
ORDER BY date ASC
