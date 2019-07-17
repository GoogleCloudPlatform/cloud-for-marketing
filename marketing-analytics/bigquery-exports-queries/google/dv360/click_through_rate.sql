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

-- This script computes the impression and click count as well as the
-- click-through rate per ad. It uses the impression ID field to join the
-- impression and click table.
WITH
  ads AS (
  SELECT
    Ad_ID,
    ANY_VALUE(Ad) As Ad
  FROM
    `project.dataset.match_table_ads`
  GROUP BY
    Ad_ID)
SELECT
  impressions.Ad_ID,
  ANY_VALUE(ads.Ad) AS Ad,
  COUNT(*) AS impression_count,
  COUNTIF(clicks.Impression_ID IS NOT NULL) AS click_count,
  COUNTIF(clicks.Impression_ID IS NOT NULL) / COUNT(*) AS click_through_rate
FROM
  `project.dataset.impression` AS impressions
LEFT JOIN
  `project.dataset.click` AS clicks
ON
  impressions.Impression_ID = clicks.Impression_ID
JOIN
  ads
ON
  impressions.Ad_ID = ads.Ad_ID
WHERE
  impressions._DATA_DATE BETWEEN DATE(2018, 5, 10)
  AND DATE(2018, 6, 10)
GROUP BY
  impressions.Ad_ID
ORDER BY
  click_through_rate DESC