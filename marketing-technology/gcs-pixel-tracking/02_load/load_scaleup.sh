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

# This is based on the ReplicationController args (currently 30000 urls in 30s at 1000/s)
# We want to get to 100000/s so we will need to go up to 100 pods with doubling every 30s
NB_PODS=100
COUNTER=1
SLEEP=15 #It takes a while to create the target.txt file
while [ $COUNTER -le  $NB_PODS ];
do
  kubectl scale deployment vegeta --replicas=$COUNTER
  echo $COUNTER
  if [ $COUNTER -eq $NB_PODS ]
    then
      echo 'break'
      break
  fi
  sleep $(($COUNTER*$SLEEP))
  COUNTER=$((2*$COUNTER))

  if [ $COUNTER -gt $NB_PODS ]
    then
      COUNTER=$NB_PODS
  fi

done
