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

-- This script can be used to compute the effective CPA for any activity and any
-- DBM line item. Currently in the DBM UI it is only possible to report on the
-- eCPA for line items and activities that have been explictly linked for bid
-- optimization. WIth this script you can report on all line items and activities.
SELECT
  DBM_Line_Item_ID,
  ANY_VALUE(DBM_Insertion_Order_ID) AS DBM_Insertion_Order_ID,
  SUM(DBM_Billable_Cost_USD) / 1000000000 AS Total_Cost_USD,
  COUNTIF(activities.DBM_Auction_ID IS NOT NULL) AS activity_count,
  (SUM(DBM_Billable_Cost_USD)  / COUNTIF(activities.DBM_Auction_ID IS NOT NULL)) / 1000000000  AS eCPA
FROM
  `project.dataset.impression` AS impressions
LEFT JOIN
  `project.dataset.activity` AS activities
ON
  impressions.DBM_Auction_ID = activities.DBM_Auction_ID
  AND
  _DATA_DATE BETWEEN DATE(2018, 5, 10) AND DATE(2018, 6, 10)
WHERE
  DBM_Billable_Cost_USD != 0
GROUP BY
  DBM_Line_Item_ID