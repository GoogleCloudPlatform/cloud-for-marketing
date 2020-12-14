#!bin/bash
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
V_DURATION="$1" #5s
V_RATE=$2 #1000
TARGETS_FILE=$3 #gcs-pixel-tracking

echo $V_DURATION
echo $V_RATE
echo $TARGETS_FILE

curl https://storage.googleapis.com/$TARGETS_FILE > /tmp/targets.txt

vegeta attack -targets=/tmp/targets.txt -duration=$V_DURATION -rate=$V_RATE | tee /tmp/report_targets.bin | vegeta report
cat /tmp/report_targets.bin | vegeta report -reporter=plot >| /tmp/plot_targets.html
