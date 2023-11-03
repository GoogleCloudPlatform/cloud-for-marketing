// Copyright 2019 Google Inc.
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

/** @fileoverview Cloud Resource Manager API handler class.*/

class CloudResourceManager extends ApiBase {

  constructor(projectId) {
    super();
    this.name = 'Cloud Resource Manager API';
    this.api = 'cloudresourcemanager.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects`;
  }

  /**
   * Gets a GCP Project.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/get
   * @return {!Project} See:
   *     https://cloud.google.com/resource-manager/reference/rest/v1/projects#Project
   */
  getProject() {
    return this.get(this.projectId);
  }

  /**
   * Returns the IAM access control policy for the specified member.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/getIamPolicy
   * @param {string} member Should start with `serviceAccount:` or `user:`, etc.
   * @return {Array<string>}
   */
  getIamPolicyForMember(member) {
    const { bindings } = this.mutate(`${this.projectId}:getIamPolicy`);
    return bindings.filter(({ members }) => members.indexOf(member) > -1)
      .map(({ role }) => role);
  }

  /**
   * Add the role(s) to the given account.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/setIamPolicy
   * @param {string} member
   * @param {string|Array<string>} addedRoles
   * @return {boolean}
   */
  addIamPolicyBinding(member, addedRoles) {
    const { version, etag, bindings } =
      this.mutate(`${this.projectId}:getIamPolicy`);
    const roles = [].concat(addedRoles);
    roles.forEach((addedRole) => {
      const roleGroup = bindings.filter(({ role }) => role === addedRole);
      if (roleGroup.length > 0) {
        if (roleGroup[0].members.indexOf(member) === -1)
          roleGroup[0].members.push(member);
      } else {
        bindings.push({
          role: addedRole,
          members: [member],
        });
      }
    });
    const payload = { policy: { version, etag, bindings } };
    const response = this.mutate(`${this.projectId}:setIamPolicy`, payload);
    return response.bindings.filter(({ role, members }) => {
      return roles.indexOf(role) > -1 && members.indexOf(member) > -1;
    }).length >= roles.length;
  }

  /**
   * Checks whether the caller has all the specified permissions.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/testIamPermissions
   * @param {Array<string>} permissions
   * @return {boolean}
   */
  testPermissions(permissions) {
    const payload = { permissions };
    const { permissions: existingPermissions } =
      this.mutate(`${this.projectId}:testIamPermissions`, payload);
    if (!existingPermissions) return false;
    return existingPermissions.length === permissions.length;
  }

  /**
   * Return active projects that the caller has the resourcemanager.projects.get
   * permission.
   * @return {{ projectId: string, createTime: Date}}
   */
  listActiveProjects() {
    const parameters = { filter: 'lifecycleState:ACTIVE' }
    let result = this.get(null, parameters);
    let projects = result.projects;
    while (result.nextPageToken) {
      console.log('Use nextPageToken to send another request', projects.length);
      parameters.pageToken = result.nextPageToken;
      result = this.get(null, parameters);
      projects = projects.concat(result.projects);
    }
    return projects
      .sort(({ createTime }) => createTime)
      .map(({ projectId, createTime }) => ({ projectId, createTime }));
  }
}
