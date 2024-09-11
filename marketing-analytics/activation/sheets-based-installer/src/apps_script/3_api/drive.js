// Copyright 2022 Google Inc.
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

/** @fileoverview Google Drive API handler class.*/

/** @const DRIVE_ROLE_MAP Drive file's role name and role vaules in API. */
const DRIVE_ROLE_MAP = {
  Viewer: 'reader',
  Commenter: 'commenter',
  Editor: 'writer',
}

class Drive extends ExternalApi {

  constructor(option) {
    super(option);
    this.name = 'Google Drive API';
    this.api = 'drive.googleapis.com';
    this.apiUrl = 'https://www.googleapis.com/drive';
    this.version = 'v3';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/drive';
  }

  /**
   * Gets the content of a Drive file.
   * @see https://developers.google.com/drive/api/reference/rest/v3/files/get
   * @param {string} fileId
   * @return {VerifyResult}
   */
  getFileContent(fileId) {
    const response = this.get(`files/${fileId}?alt=media`);
    if (response.error) {
      throw new Error(error.message);
    }
    return response;
  }

  /**
   * Gets the information of a Drive file.
   * @see https://developers.google.com/drive/api/reference/rest/v3/files/get
   * @param {string} fileId
   * @return {VerifyResult}
   */
  getFileInfo(fileId) {
    const response = this.get(`files/${fileId}`);
    if (response.error) {
      throw new Error(error.message);
    }
    return response;
  }

  /**
   * Creates or updates a permission for a file.
   * @see https://developers.google.com/drive/api/reference/rest/v3/permissions/create
   * @param {string} fileId
   * @param {string} email
   * @param {string} role 'Viewer', 'Commenter' or 'Editor'.
   * @return {Permission}
   */
  addUser(fileId, email, role) {
    const payload = {
      type: 'user',
      emailAddress: email,
      role: DRIVE_ROLE_MAP[role] || 'reader',
    }
    const response = this.mutate(`files/${fileId}/permissions`, payload);
    if (response.role !== role) {
      console.log(`User ${email} has role`, response.role);
    }
    return response;
  }
}
