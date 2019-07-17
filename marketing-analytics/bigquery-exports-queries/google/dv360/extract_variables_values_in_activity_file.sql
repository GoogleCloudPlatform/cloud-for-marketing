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
-- How do I extract additional values that I pass through u-variables in conversion data?
--
-- What can I do?
-- Analyze conversion data at a more granular level based on client-provided data. For example, analyze the most common conversion paths for a specific product.
--
-- Tips
-- Use REGEXP_EXTRACT to extract u-variable data from the “Other_Data” field.  Create new columns for each extracted u-variable.

SELECT
  TRIM(REGEXP_EXTRACT(Other_Data, r"u4=(.+?);")) AS Product_Purchased,
  COUNT(*) AS Conversions
FROM `project.dataset.activity`
WHERE Activity_ID IN ("12345","56789")
GROUP BY 1
ORDER BY 2 DESC
