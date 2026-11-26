// Copyright 2026 Google Inc.
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

/** @fileoverview MojoTemplate for time zone resource. */
MojoTemplateRegistry.register('timeZone', {
  category: 'General',
  resource: 'Time Zone',
  propertyName: 'timeZone',
  editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
  value_datarange: TIMEZONES,
  checkFn: timeZone => {
    const result = TIMEZONES.indexOf(timeZone) > -1;
    if (result) return {status: RESOURCE_STATUS.OK};
    const message = 'Could not find the time zone.';
    return {status: RESOURCE_STATUS.ERROR, message};
  },
});
