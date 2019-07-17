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
-- This script calculates the average number of product page views by purchaser type (purchasers vs non-purchasers)


SELECT
( SUM(total_pagesviews_per_user) / COUNT(users) ) AS avg_pageviews_per_user
FROM
SELECT
fullVisitorId AS users,
SUM(totals.pageviews) AS total_pagesviews_per_user
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
AND
totals.transactions >=1
GROUP BY
users )
