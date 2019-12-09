// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.corp.gtech.ads.datacatalyst.components.mldatawindowingpipeline.model;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** A Field records a feature column name, description and type. */
public class Field implements Serializable {

  private final String name;
  private final String description;
  private final String type;

  public Field(String name, String description, String type) {
    this.name = name;
    this.description = description;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getType() {
    return type;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Field)) {
      return false;
    }
    Field otherField = (Field) other;
    return Objects.equals(this.name, otherField.name)
        && Objects.equals(this.type, otherField.type)
        && Objects.equals(this.description, otherField.description);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {name, description, type});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(name)
        .addValue(description)
        .addValue(type)
        .toString();
  }
}
