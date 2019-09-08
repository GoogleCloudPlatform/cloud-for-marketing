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
-- How do I determine whether an event happened on mobile vs desktop for DBM impressions?
--
-- What can I do?
-- Add mobile vs desktop breakouts in your analyses.
--
-- Tips
-- * Include this with other queries to get performance, reach, and more by DBM Device Type.

SELECT
  DBM_Device_Type,
  CASE
    WHEN DBM_Device_Type=0 THEN "Computer"
    WHEN DBM_Device_Type=1 THEN "Other"
    WHEN DBM_Device_Type=2 THEN "Smartphone"
    WHEN DBM_Device_Type=3 THEN "Tablet"
    WHEN DBM_Device_Type=4 THEN "Smart TV"
  END DBM_Device_Type_Name,
  COUNT(*) AS Impressions
FROM `project.dataset.impression`
WHERE DBM_Advertiser_ID IS NOT NULL
GROUP BY DBM_Device_Type, DBM_Device_Type_Name
ORDER BY Impressions DESC
