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
  /**
   * @param {string=} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'Cloud Resource Manager API';
    this.api = 'cloudresourcemanager.googleapis.com';
    this.permissions = [
      'resourcemanager.projects.get',
      'resourcemanager.projects.setIamPolicy',
      'iam.serviceAccounts.actAs',
    ];
    this.adminRole = 'Project IAM Admin';
    this.runtimeRoles = [];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /**
   * @override
   * @return {string}
   */
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
   * Returns the numeric GCP Project Number for the current project.
   * @return {string} Project Number as string (e.g. "123456789012").
   */
  getProjectNumber() {
    const project = this.getProject();
    if (!project || !project.projectNumber) {
      throw new Error(
        `Unable to resolve projectNumber for project "${this.projectId}"`,
      );
    }
    return String(project.projectNumber);
  }

  /**
   * Returns the default Compute Engine Service Account for this project.
   * Format: <PROJECT_NUMBER>-compute@developer.gserviceaccount.com
   * @return {string}
   */
  getDefaultComputeServiceAccount() {
    const projectNumber = this.getProjectNumber();
    return `${projectNumber}-compute@developer.gserviceaccount.com`;
  }

  /**
   * Returns the IAM access control policy for the specified member.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/getIamPolicy
   * @param {string} member Should start with `serviceAccount:` or `user:`, etc.
   * @return {!Array<string>}
   */
  getIamPolicyForMember(member) {
    const {bindings} = this.mutate(`${this.projectId}:getIamPolicy`);
    return bindings
      .filter(({members}) => members.indexOf(member) > -1)
      .map(({role}) => role);
  }

  /**
   * Add the role(s) to the given account.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/setIamPolicy
   * @param {string} member
   * @param {string|!Array<string>} addedRoles
   * @return {boolean}
   */
  addIamPolicyBinding(member, addedRoles) {
    const {version, etag, bindings} = this.mutate(
      `${this.projectId}:getIamPolicy`,
    );
    const roles = [].concat(addedRoles);
    roles.forEach(addedRole => {
      const roleGroup = bindings.filter(({role}) => role === addedRole);
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
    const payload = {policy: {version, etag, bindings}};
    const response = this.mutate(`${this.projectId}:setIamPolicy`, payload);
    return (
      response.bindings.filter(({role, members}) => {
        return roles.indexOf(role) > -1 && members.indexOf(member) > -1;
      }).length >= roles.length
    );
  }

  /**
   * Checks whether the caller has all the specified permissions.
   * @see https://cloud.google.com/resource-manager/reference/rest/v1/projects/testIamPermissions
   * @param {!Array<string>} permissions
   * @return {boolean}
   */
  testPermissions(permissions) {
    const payload = {permissions};
    const {permissions: existingPermissions} = this.mutate(
      `${this.projectId}:testIamPermissions`,
      payload,
    );
    if (!existingPermissions) return false;
    return existingPermissions.length === permissions.length;
  }

  /**
   * Returns a link to the GCP project home page in Google Cloud Console.
   * @return {string}
   */
  getLinkToCloudConsole() {
    const link = `https://console.cloud.google.com/home?project=${this.projectId}`;
    return link;
  }

  /**
   * Returns a link to the GCP IAM page in Google Cloud Console.
   * @return {string}
   */
  getLinkToIam() {
    const link = `https://console.cloud.google.com/iam-admin/iam??project=${
      this.projectId
    }`;
    return link;
  }

  /**
   * Returns a link to the GCP web page about this permission.
   * @param {string} permission
   * @return {string}
   */
  getLinkToPermissionInfo(permission) {
    const resource = permission.split('.')[0];
    const link = `https://cloud.google.com/iam/docs/roles-permissions/${
      resource
    }#${permission}`;
    return link;
  }

  /**
   * Return active projects that the caller has the resourcemanager.projects.get
   * permission.
   * @return {{ projectId: string, createTime: !Date}}
   */
  listActiveProjects() {
    const parameters = {filter: 'lifecycleState:ACTIVE'};
    const projects = this.list('projects', null, parameters);
    return projects
      .sort(({createTime}) => createTime)
      .map(({projectId, createTime}) => ({projectId, createTime}));
  }
}
