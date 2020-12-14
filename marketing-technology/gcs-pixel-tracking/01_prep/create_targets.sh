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
#
# bash create_targets.sh LB_IP NB_URLS OUTPUT_FILE

PIXEL_DOMAIN_OR_IP="$1"
NB_URLS=$2
OUTPUT="$3"

DATA_PATH=data
rm $3
u=1
while [[ $u -le $NB_URLS ]]
do
  # Create the simple params
  PAGE_NAME=$(gshuf -n 1 "$DATA_PATH/page_names.txt")
  EVENT=$(gshuf -n 1 "$DATA_PATH/events.txt")

  # Create the url param http%3A%2F%2Fexample.com%2Fcart
  RANDOM_DOMAIN=$(gshuf -n 1 "$DATA_PATH/domains.txt")
  PAGE_URL="http%3A%2F%2F$RANDOM_DOMAIN%2F$PAGE_NAME"

  # Create the product string which has at least one product for the product
  # page and an extra possibl3 if in page related to cart
  #declare -a PAGES_NEED_PRODUCT=(cart shopping_cart checkout products)
  PAGES_NEED_PRODUCT[0]="cart"
  PAGES_NEED_PRODUCT[1]="shopping_cart"
  PAGES_NEED_PRODUCT[2]="checkout"
  PAGES_NEED_PRODUCT[3]="products"

  if [[ " ${PAGES_NEED_PRODUCT[*]} " == *"$PAGE_NAME"* ]]; then
    RANDOM_PRODUCT=$(gshuf -n 1 "$DATA_PATH/products.txt")
    PRODUCT="&pr=$RANDOM_PRODUCT;"
    if [ "$PAGE_NAME" != "products" ]; then
      # Add random amount of product (between 1 and 3)
      ADD_X_PRODUCTS=$(( ( RANDOM % 3 )  + 1 ))
      RANDOM_X_PRODUCTS=$(gshuf -n $ADD_X_PRODUCTS "$DATA_PATH/products.txt")
      for i in ${RANDOM_X_PRODUCTS[@]};
      do
        PRODUCT+="${i};"
      done
    fi
    # Remove the last ";"
    PRODUCT=${PRODUCT%?}
  fi

  # Create a random user id between 10000 and 100000
  USER_ID=$(( ( RANDOM%90000 ) + 10000 ))
  echo "GET http://$PIXEL_DOMAIN_OR_IP/pixel.png?uid=$USER_ID&pn=$PAGE_NAME&purl=$PAGE_URL&e=${EVENT}${PRODUCT}" >> $3
  ((u = u + 1))
done
