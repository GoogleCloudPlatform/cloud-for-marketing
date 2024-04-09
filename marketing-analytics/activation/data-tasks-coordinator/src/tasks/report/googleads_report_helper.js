// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/**
 * @fileoverview Facilitator functions to generate BigQuery table schema from
 * Google Ads reports.
 * @link getSchemaFields
 */

'use strict';
const lodash = require('lodash');
const {
  api: { googleadsapi: { GoogleAdsField } },
  utils: { changeNamingFromSnakeToLowerCamel },
} = require('@google-cloud/nodejs-common');

/**
 * Definition of the field in BigQuery schema.
 * @typedef {{
 *   name:string,
 *   type:string,
 *   mode:'REPEATED'|undefined,
 *   fields:Array<FieldInSchema>|undefined,
 * }}
 */
let FieldInSchema;

/**
 * Some fields of GAds report have the type as 'message', which are
 * objects defined in Google Ads API. Those detailed properties don't show up at
 * the GoogleAdsFieldService. They will fail the load job to BigQuery without
 * detailed definition in schema.
 * So we need to manually define the (required) fields in snake case to align
 * with the responses of Google Ads API.
 * Note, only needed fields are mapped.
 *
 * @type {{string: Array<FieldInSchema>}}
 */
const GOOGLE_ADS_MESSAGES = {
  // https://developers.google.com/google-ads/api/reference/rpc/latest/AdTextAsset
  AdTextAsset: [
    {name: 'text', type: 'STRING',},
    {name: 'pinned_field', type: 'STRING',},
  ],
  // https://developers.google.com/google-ads/api/reference/rpc/latest/PolicyTopicEntry
  PolicyTopicEntry: [
    {name: 'topic', type: 'STRING',},
    {name: 'type', type: 'STRING',},
  ],
  // https://developers.google.com/google-ads/api/reference/rpc/latest/AdGroupAdAssetPolicySummary
  AdGroupAdAssetPolicySummary: [
    {
      name: 'policy_topic_entries',
      mode: 'REPEATED',
      type: 'RECORD',
      fields: [
        {name: 'topic', type: 'STRING',},
        {name: 'type', type: 'STRING',},
      ],
    },
    {name: 'review_status', type: 'STRING',},
    {name: 'approval_status', type: 'STRING',},
  ],
  // https://developers.google.com/google-ads/api/reference/rpc/latest/ChangeEvent.ChangedResource
  ChangedResource: [
    {
      name: 'campaign',
      type: 'RECORD',
      fields: [
        {
          name: 'target_cpa',
          type: 'RECORD',
          fields: [{name: 'target_cpa_micros', type: 'INT64'}],
        },
      ],
    },
    {
      name: 'campaign_budget',
      type: 'RECORD',
      fields: [{name: 'amount_micros', type: 'INT64'}],
    },
  ],
};

/**
 * By default, the mapped fields in snake_case. This function returns the mapped
 * fields in lower camel case.
 * @see GOOGLE_ADS_MESSAGES
 * @param {!Array<!FieldInSchema>} mappedFields
 * @return {!Array<!FieldInSchema>}
 */
const convertMappedMessageCase = (mappedFields) => {
  return mappedFields.map((field) => {
    const newField = lodash.merge({}, field,
      { name: changeNamingFromSnakeToLowerCamel(field.name) })
    if (field.fields) {
      newField.fields = convertMappedMessageCase(field.fields);
    }
    return newField;
  })
}

/**
 * Gets the mapped messages based on specified case type.
 * @param {boolean=} snakeCase
 * @return {!Array<!FieldInSchema>}
 */
const getMappedMessages = (snakeCase = false) => {
  if (snakeCase === true) return GOOGLE_ADS_MESSAGES;
  const GOOGLE_ADS_MESSAGES_CAMEL_CASE = {};
  Object.keys(GOOGLE_ADS_MESSAGES).forEach((key) => {
    GOOGLE_ADS_MESSAGES_CAMEL_CASE[key] =
      convertMappedMessageCase(GOOGLE_ADS_MESSAGES[key]);
  });
  return GOOGLE_ADS_MESSAGES_CAMEL_CASE;
}


/**
 * Maps the array of AdsFields to a structured object. e.g. AdsFields array
 * [
 *   metrics.clicks,
 *   metrics.conversions,
 *   ad_group_ad.ad.name,
 * ] turns to be:
 * {
 *   metrics:{
 *     clicks:{},
 *     conversions:{},
 *   },
 *   ad_group_ad:{
 *     ad:{
 *       name:{},
 *     },
 *   },
 * }
 * @param {!Array<string>} adsFieldNames
 * @return {object} The structured object.
 */
const mapAdsFieldsToObject = (adsFieldNames) => {
  const structuredAdFields = {};
  adsFieldNames.forEach((adsFieldName) => {
    adsFieldName.split('.').reduce(((previousValue, currentValue) => {
      if (!previousValue[currentValue]) previousValue[currentValue] = {};
      return previousValue[currentValue];
    }), structuredAdFields);
  });
  return structuredAdFields;
};

/**
 * Get BigQuery data type from the GoogleAdsFieldDataType.
 * @see https://developers.google.com/google-ads/api/reference/rpc/v7/GoogleAdsFieldDataTypeEnum.GoogleAdsFieldDataType
 * @param {GoogleAdsFieldDataType} dateType
 * @return {string} BigQuery data type. See:
 *     https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
 */
const getBigQueryDataType = (dateType) => {
  switch (dateType) {
    case 'BOOLEAN':
      return 'BOOL';
    case 'DATE':
      return 'DATETIME';
    case 'STRING':
    case 'RESOURCE_NAME':
    case 'ENUM':
      return 'STRING';
    case 'INT32':
    case 'INT64':
      return 'INT64';
    case 'DOUBLE':
    case 'FLOAT':
      return 'FLOAT64';
    case 'MESSAGE':
      return 'RECORD';
    default:
      throw new Error(`Unknown date type: ${dateType}`);
  }
};

/**
 * Maps a single GoogleAdsField to a field in BigQuery load schema. For those
 * 'Message' typed GoogleAdsFields whose types in BigQuery are 'RECORD', extra
 * configuration(mappedTypes) of its fields are required.
 * @link {GOOGLE_ADS_MESSAGES}
 * @param {string} name Field name in LowerCamelCase format.
 * @param {!GoogleAdsField} adsField
 * @param {{string:Array<FieldInSchema>}} mappedTypes @link GOOGLE_ADS_MESSAGES
 * @return {!FieldInSchema}
 */
const getSingleField = (name, adsField, mappedTypes) => {
  const type = getBigQueryDataType(adsField.dataType);
  const field = {name, type};
  if (adsField.isRepeated) field.mode = 'REPEATED';
  if (type === 'RECORD') {
    const types = adsField.typeUrl.split('.');
    const fields = mappedTypes[types[types.length - 1]];
    if (!fields) throw new Error(`${adsField.typeUrl} isn't defined.`);
    field.fields = fields;
  }
  return field;
};

/**
 * Transforms an array of GoogleAdsFields in to the array of fields in BigQuery
 * schema. For example:
 * from an array of GoogleAdsField like this:
 * [
 *   metrics.clicks,
 *   metrics.conversions,
 *   ad_group_ad.ad.name,
 * ]
 *
 * To an array of fields in the schema for load reports to BigQuery like this:
 * [
 *   {name:'metrics', type:'RECORD', fields:[
 *     {name:'clicks', type:'INT64'},
 *     {name:'conversions', type:'FLOAT64'},
 *   ]},
 *   {name:'ad_group_ad', type:'RECORD', fields:[
 *     {name:'ad', type:'RECORD', fields:[
 *        {name:'name', type:'STRING'},
 *     ]},
 *   ]},
 * ]
 *
 * To understand more about:
 * 1. Google Ads segments:
 *    https://developers.google.com/google-ads/api/fields/latest/segments
 * 2. Google Ads metrics:
 *    https://developers.google.com/google-ads/api/fields/latest/metrics
 * 3. Google Ads resources:
 *    https://developers.google.com/google-ads/api/reference/rpc/latest/overview
 * 4. BigQuery data type:
 *    https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
 *
 * @param {!Array<string>} adsFieldNames
 * @param {!Array<!GoogleAdsField>} adsFields
 * @param {boolean=} snakeCase Default value False
 * @return {!Array<FieldInSchema>}
 */
const getSchemaFields = (adsFieldNames, adsFields, snakeCase = false) => {
  const adsFieldsMap = {};
  adsFields.forEach((adsField) => {
    const key = snakeCase
      ? adsField.name : changeNamingFromSnakeToLowerCamel(adsField.name);
    adsFieldsMap[key] = adsField;
  });
  const finalFieldNames = snakeCase
    ? adsFieldNames : adsFieldNames.map(changeNamingFromSnakeToLowerCamel);
  const mappedTypes = getMappedMessages(snakeCase);

  /**
   * Map an array of GoogleAdsFields to an array of fields in BigQuery load
   * schema.
   * @param {string} key BigQuery field name.
   * @param {object} value The value of the key in the structured object. See
   *     {@link mapAdsFieldsToObject}
   * @param {string} prefix Prefix part of this field in GoogleAdsField name.
   * @return {FieldInSchema}
   */
  const getSchemaFromObject = (key, value, prefix) => {
    const newPrefix = prefix ? `${prefix}.${key}` : key;
    const name = key;
    if (Object.keys(value).length === 0) {
      return getSingleField(name, adsFieldsMap[newPrefix], mappedTypes);
    }
    return {
      name,
      type: 'RECORD',
      fields: Object.keys(value).map(
          (subKey) => getSchemaFromObject(subKey, value[subKey], newPrefix)),
    };
  };
  const structuredAdFields = mapAdsFieldsToObject(finalFieldNames);
  return Object.keys(structuredAdFields).map(
      (key) => getSchemaFromObject(key, structuredAdFields[key]));
}

module.exports = {getSchemaFields};
