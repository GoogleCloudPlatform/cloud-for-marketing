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

-- I want to learn...
-- How do I join impression, click, and conversion data at an aggregated level (e.g. campaign level)?
--
-- What can I do?
-- Automate custom reporting tables that power business insights dashboards.
--
-- Tips
-- * Try replicating this method at other aggregation levels such as Advertiser, Site, Placement, Ad, Creative, etc.
-- * Matching to the activity table using Advertiser ID, Campaign ID, Site, ID, etc. will assume your default attribution methodology and lookback windows like in DCM UI Reporting.
-- * This method is the simplest method to join impression, click, and activity tables, but is less flexible.  Other join methods include joining with Impression ID or User ID (see following slides).

WITH impression_data AS (
  SELECT
    Campaign_ID,
    COUNT(*) AS Impressions
  FROM `project.dataset.impression`
  GROUP BY Campaign_ID
),

click_data AS (
  SELECT
    Campaign_ID,
    COUNT(*) AS Clicks
  FROM `project.dataset.click`
  GROUP BY Campaign_ID
),

conversion_data AS (
  SELECT
    Campaign_ID,
    COUNT(*) AS Conversions
  FROM `project.dataset.activity`
  WHERE Activity_ID IN ("12345","67890")
  GROUP BY Campaign_ID
)

SELECT
  *,
  Clicks/Impressions AS Click_Rate,
  Conversions/Impressions AS Conversion_Rate
FROM impression_data
LEFT JOIN click_data USING(Campaign_ID)
LEFT JOIN conversion_data USING(Campaign_ID)
ORDER BY Impressions DESC
