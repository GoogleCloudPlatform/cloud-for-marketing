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

/**
 * Returns a default location for Firestore based on the property 'locationId'.
 * The property 'locationId' is selected by users for Cloud Functions/Storage.
 * For better performance, a default Firestore location is decided to be the
 * same or close to that location.
 * @param {string} locationId
 * @param {!Array<string>} locations
 * @return {{displayName:string, locationId: string}}
 * @private
 */
MojoFunctions.getFirestoreDefaultLocationObject_ = (locationId, locations) => {
  let id;
  if (locationId.startsWith('europe-')) {
    id = 'eur3';
  } else if (locationId.startsWith('us-')) {
    id = 'nam5';
  } else {
    const target = locations.filter(({locationId: id}) => locationId === id)[0];
    id = target ? target.locationId : 'nam5';
  }
  return getLocationObject(locations, id);
};

/**
 * Checks Firestore status. If it exists, return status `OK` and its location;
 * if it doesn't exist, return `TO_APPLY` to ask the use to continue to
 * create the Firestore at the 'selected location' (from the `attributeValue` of
 * the `resource`) or the default location (from the document property).
 * Used property: projectId, locationId.
 * @param {string} databaseId
 * @param {!Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.checkFirestore = (databaseId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const {attributes, attributesMap} = resource;
  const attributeNames = Firestore.ATTRIBUTE_NAMES;
  const modeAttr = attributesMap[attributeNames.mode];
  const locationAttr = attributesMap[attributeNames.location];

  const selectedMode = modeAttr.attributeValue;
  const selectedLocation = locationAttr.attributeValue;

  const firestore = new Firestore(projectId, databaseId);

  let listLocationError;
  let locations;
  try {
    locations = firestore.listLocations();
    locationAttr.attributeValue_datarange = locations.map(getLocationListName);
  } catch (error) {
    console.warn('Failed to list Firestore locations', error);
    listLocationError = error;
  }

  const {error, type, locationId} = firestore.getDatabase();
  if (error) {
    if (error.status === 'NOT_FOUND') {
      // Only show error for list location when need to create a datastore
      if (listLocationError) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: listLocationError.message,
          attributes,
        };
      }
      const locationId = getDocumentProperty('locationId');
      const targetMode = selectedMode || FIRESTORE_MODE.FIRESTORE_NATIVE;
      const targetLocation =
        selectedLocation ||
        getLocationListName(
          MojoFunctions.getFirestoreDefaultLocationObject_(
            locationId,
            locations,
          ),
        );
      modeAttr.attributeValue = targetMode;
      locationAttr.attributeValue = targetLocation;
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: `Will create a ${targetMode} Firestore[${databaseId}] in ${
          targetLocation
        }`,
        attributes,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
      attributes,
    };
  }
  const currentLocation = listLocationError
    ? locationId
    : getLocationListName(getLocationObject(locations, locationId));
  modeAttr.attributeValue = FIRESTORE_MODE[type];
  locationAttr.attributeValue = currentLocation;
  return {
    status: RESOURCE_STATUS.OK,
    message:
      selectedMode === FIRESTORE_MODE[type] &&
      selectedLocation === currentLocation
        ? ''
        : 'Updated mode and location to the existing Firestore in this GCP',
    attributes,
    status_link: firestore.getLinkToCloudConsole(
      type,
      getDocumentProperty('namespace'),
    ),
  };
};

/**
 * Creates a Firestore instance.
 * Used property: projectId.
 * @param {string} databaseId
 * @param {!Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.createFirestore = (databaseId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const {attributesMap} = resource;
  const modeDesc = attributesMap['Mode'].attributeValue;
  const selectedLocation = attributesMap['Location'].attributeValue;

  const locationId = getLocationId(selectedLocation);
  const mode = Object.keys(FIRESTORE_MODE).filter(
    key => FIRESTORE_MODE[key] === modeDesc,
  )[0];
  const firestore = new Firestore(projectId, databaseId);
  const response = firestore.createDatabase(locationId, mode, databaseId);
  const {done, error} = response;
  if (error) return {status: RESOURCE_STATUS.ERROR, message: error.message};
  if (!done) console.warn('Incomplete create Firestore response', response);
  return {
    status: RESOURCE_STATUS.OK,
    message: 'Firestore instance created.',
    status_link: firestore.getLinkToCloudConsole(
      mode,
      getDocumentProperty('namespace'),
    ),
  };
};

/**
 * Parses raw fields string or object array into structured Firestore index
 * field specs.
 * @private
 * @param {*} rawValue
 * @return {!Array<!Object>}
 */
MojoFunctions.parseFirestoreIndexFields_ = rawValue => {
  if (Array.isArray(rawValue)) return rawValue;
  if (!rawValue || typeof rawValue !== 'string') return [];

  const trimmed = rawValue.trim();
  if (trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed);
    } catch (e) {
      console.warn(
        'Failed to parse JSON for Firestore index fields:',
        rawValue,
      );
    }
  }

  const items = trimmed
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  return items.map(item => {
    const parts = item.split(/[:\s]+/).filter(Boolean);
    const fieldPath = parts[0];
    let order = (parts[1] || 'ASC').toUpperCase();
    if (order === 'ASC') order = 'ASCENDING';
    if (order === 'DESC') order = 'DESCENDING';
    return {fieldPath, order};
  });
};

/**
 * Combined check and idempotent create/update for Firestore Composite Indexes.
 * @param {string} collectionGroupId Target collection group identifier.
 * @param {!Object} resources The resources object containing attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateFirestoreCompositeIndex = (
  collectionGroupId,
  resources,
) => {
  const projectId = getDocumentProperty('projectId');
  const databaseId = getDocumentProperty('databaseId') || '(default)';
  const firestore = new Firestore(projectId, databaseId);

  const {error: dbError} = firestore.getDatabase();
  if (dbError) {
    if (dbError.status === 'NOT_FOUND' || dbError.code === 404) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: `Firestore database [${databaseId}] is not ready yet.`,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: dbError.message,
    };
  }

  const fields = MojoFunctions.parseFirestoreIndexFields_(
    resources.attributeValue,
  );
  const statusLink = firestore.getLinkToCloudConsole(
    'FIRESTORE_NATIVE',
    collectionGroupId,
  );

  try {
    const result = firestore.createOrUpdateCompositeIndex(
      collectionGroupId,
      fields,
    );
    if (result.error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Failed to create or update Firestore Composite Index for [${
          collectionGroupId
        }]: ${result.error.message || JSON.stringify(result.error)}`,
        status_link: statusLink,
      };
    }
    if (result.existed) {
      return {
        status: RESOURCE_STATUS.OK,
        message: `Firestore Composite Index on collection [${
          collectionGroupId
        }] already exists (${result.state}).`,
        status_link: statusLink,
      };
    }
    return {
      status: RESOURCE_STATUS.OK,
      message: `Created Firestore Composite Index on collection [${
        collectionGroupId
      }].`,
      status_link: statusLink,
    };
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to create or update Firestore Composite Index for [${
        collectionGroupId
      }]: ${error.message || JSON.stringify(error)}`,
      status_link: statusLink,
    };
  }
};
/**
 * Saves entities to Firestore for both native modes.
 * @param {string} kind Entity name.
 * @param {!Object<string,!Object>} entities
 * @param {string=} path The path of the Firestore instance.
 * @return {!Firestore.Commit}
 */
MojoFunctions.saveEntitiesToFirestore = (kind, entities, path = '') => {
  const projectId = getDocumentProperty('projectId');
  const databaseId = getDocumentProperty('databaseId');
  const firestore = new Firestore(projectId, databaseId);
  const {error, type} = firestore.getDatabase();
  if (error) {
    throw new Error(error.message);
  }
  if (type === 'DATASTORE_MODE') {
    throw new Error('Datastore mode is not supported.');
  } else {
    const firestore = new Firestore(projectId, databaseId, path, kind);
    const response = firestore.txSave(entities);
    return response.writeResults;
  }
};

/**
 * Returns documents/entities from Firestore for the specified kind.
 * @param {string} kind Entity name.
 * @param {string=} path The path of the Firestore instance.
 * @return {!Array<{
 *   id: string,
 *   json: !Object,
 * }>}
 */
MojoFunctions.loadEntitiesFromFirestore = (kind, path = '') => {
  const projectId = getDocumentProperty('projectId');
  const databaseId = getDocumentProperty('databaseId');
  const firestore = new Firestore(projectId, databaseId);
  const {error, type} = firestore.getDatabase();
  if (error) {
    throw new Error(error.message);
  }
  if (type === 'DATASTORE_MODE') {
    throw new Error('Datastore mode is not supported.');
  } else {
    const firestore = new Firestore(projectId, databaseId, path, kind);
    const response = firestore.listDocuments();
    return response;
  }
};

MojoTemplateRegistry.register('firestore', {
  category: 'Google Cloud',
  resource: 'Firestore',
  value: DEFAULT_DATABASE,
  attributes: [
    {
      attributeName: Firestore.ATTRIBUTE_NAMES.mode,
      attributeValue_datarange: Object.values(FIRESTORE_MODE),
    },
    {
      attributeName: Firestore.ATTRIBUTE_NAMES.location,
    },
  ],
  propertyName: 'databaseId',
  checkFn: MojoFunctions.checkFirestore,
  enableFn: MojoFunctions.createFirestore,
});

MojoTemplateRegistry.register('firestoreIndex', {
  category: 'Google Cloud',
  resource: 'Firestore Index',
  value: '${collectionGroupId}',
  attributeName: Firestore.INDEX_ATTRIBUTE_NAMES.fields,
  checkFn: MojoFunctions.checkOrUpdateFirestoreCompositeIndex,
});
