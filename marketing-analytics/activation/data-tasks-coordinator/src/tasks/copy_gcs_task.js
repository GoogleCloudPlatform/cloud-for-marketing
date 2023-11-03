// Copyright 2021 Google Inc.
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
 * @fileoverview Task to copy files between Cloud Storage buckets.
 */

'use strict';

const {Bucket, File} = require('@google-cloud/storage');
const {
  TaskType,
  StorageFileConfig,
} = require('../task_config/task_config_dao.js');

const {BaseTask} = require('./base_task.js');

/**
 * Options for copying Cloud Storage file(s).
 * 1. deleteSource, delete the source after copying. Default value: false.
 * 2. noSubFolder, don't copy files in sub folders. Default value: false.
 *   Note: A trailing '/' is required if the 'name' in source is a folder name.
 * @typedef {{
 *   deleteSource:boolean|undefined,
 *   noSubFolder:boolean|undefined,
 * }}
 */
let CopyGcsOption;

/**
 *
 * @typedef {{
 *   type:TaskType.COPY_GCS,
 *   source: !StorageFileConfig,
 *   destination: !StorageFileConfig,
 *   options: !CopyGcsOption,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let CopyGcsTaskTaskConfig;

class CopyGcsTask extends BaseTask {

  /** @override */
  async doTask() {
    const {/** @const {!StorageFileConfig} */ destination} = this.config;
    /** @const {!Bucket} */
    const target = this.getStorage(destination).bucket(destination.bucket);
    /** @const {!Array<!File>} */
    const files = await this.getFiles_();
    this.copyPromises = files.map((file) => {
      return new Promise((resolve, reject) => {
        const outputStream = target.file(file.name).createWriteStream();
        file.createReadStream().pipe(outputStream)
            .on('error', (error) => reject(error))
            .on('finish', () => {
              this.logger.debug(
                  `Copying ${file.name} to ${destination.bucket}`);
              resolve(true);
            });
      });
    });
    return {numberOfCopiedFiles: files.length};
  }

  /** @override */
  async isDone() {
    if (!this.copyPromises) {
      this.logger.error('No copying process exists. Wrong status.');
      throw new Error('Wrong status of COPY_GCS task.');
    }
    await Promise.all(this.copyPromises);
    this.logger.debug('All files are copied.');
    return true;
  }

  /** @override */
  async completeTask() {
    const {options} = this.config;
    if (options && options.deleteSource) {
      this.logger.debug(`Need to delete source file(s).`);
      /** @const {!Array<!File>} */
      const files = await this.getFiles_();
      await Promise.all(files.map((file) => {
        this.logger.debug(`Delete ${file.name}.`);
        return file.delete();
      }));
    }
    return {
      parameters: this.appendParameter(
          {
            destinationFile: {
              bucket: this.config.destination.bucket,
              name: this.config.source.name,
            }
          }),
    }
  }

  /**
   * Get the array of Storage Files from the 'source' configuration.
   * It returns all files with the 'name' as prefix of the file names in that
   * bucket.
   * @return {!Promise<!Array<!File>>}
   * @private
   */
  async getFiles_() {
    const {/** @const {!StorageFileConfig} */ source} = this.config;
    const filter = {prefix: source.name};
    if (this.config.options && this.config.options.noSubFolder) {
      filter.delimiter = '/';
    }
    const [files] = await this.getStorage(source).bucket(source.bucket)
        .getFiles(filter);
    this.logger.debug(
        `Get ${files.length} files in ${source.bucket}/${source.name}`);
    return files;
  }
}

module.exports = {
  CopyGcsTaskTaskConfig,
  CopyGcsTask,
};
