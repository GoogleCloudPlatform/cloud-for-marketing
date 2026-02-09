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

const unzipper = require('unzipper');
const stripBomStream = require('strip-bom-stream').default;
const path = require('path');
const { Bucket, File } = require('@google-cloud/storage');
const {
  TaskType,
  StorageFileConfig,
} = require('../../task_config/task_config_dao.js');

const { BaseTask } = require('../base_task.js');

/**
 * Options for copying Cloud Storage file(s).
 * 1. deleteSource, delete the source after copying. Default value: false.
 * 2. noSubFolder, don't copy files in sub folders. Default value: false.
 *   Note: A trailing '/' is required if the 'name' in source is a folder name.
 * 3. unzip, if the source is a zip file, it would unzip it to the target.
 * 4. stripBom, whether or not strip the BOM from the copied files.
 * @typedef {{
 *   deleteSource:boolean|undefined,
 *   noSubFolder:boolean|undefined,
 *   unzip:boolean|undefined,
 *   stripBom:boolean|undefined,
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
    const {
      /** @const {!StorageFileConfig} */ destination,
      options = {},
    } = this.config;
    /** @const {!Bucket} */
    const targetBucket = this.getStorage(destination).bucket(destination.bucket);
    const targetFolder = destination.name || '';
    /** @const {!Array<!File>} */
    const files = await this.getFiles_();
    this.copyPromises = files.map((file) => {
      if (options.unzip && file.name.toLocaleLowerCase().endsWith('.zip')) {
        return this.getUnzipPromise_(file, targetBucket, targetFolder);
      } else {
        return new Promise((resolve, reject) => {
          const outputStream = targetBucket.file(
            path.join(targetFolder, file.name)).createWriteStream();
          const sourceStream = options.stripBom
            ? file.createReadStream().pipe(stripBomStream())
            : file.createReadStream();
          sourceStream.pipe(outputStream)
            .on('error', reject)
            .on('finish', () => {
              this.logger.debug(
                `Copied ${file.name} to ${destination.bucket}/${targetFolder}`);
              resolve(true);
            });
        });
      };
    });
    return { numberOfCopiedFiles: files.length };
  }

  /**
   * Unzips a ZIP file from GCS and uploads the contents back to a target GCS
   * folder. This method uses streaming to avoid downloading the entire archive
   * to memory or disk.
   * Flow:
   * Read ZIP Stream -> Parse Entries -> (Optional) Strip BOM -> Write to GCS.
   * @param {File} zipFile - Source file object (google-cloud/storage File).
   * @param {Bucket} targetBucket - Destination bucket object (google-cloud/storage Bucket).
   * @param {string} targetFolder - Destination folder path within the bucket.
   * @returns {Promise<boolean>} Resolves to true when all files are unzipped and uploaded.
   * @private
   */
  async getUnzipPromise_(zipFile, targetBucket, targetFolder) {
    const promises = [];
    const stripBom = this.config.options ? this.config.options.stripBom : false;
    return new Promise((resolve, reject) => {
      zipFile.createReadStream().pipe(unzipper.Parse())
        .on('entry', (entry) => {
          const fileName = entry.path;
          const type = entry.type;
          if (type === 'File') {
            const uploadPromise = new Promise((res, rej) => {
              const outputStream = targetBucket.file(
                path.join(targetFolder, fileName)).createWriteStream();
              const sourceStream = stripBom ? entry.pipe(stripBomStream()) : entry;
              sourceStream.pipe(outputStream)
                .on('finish', () => {
                  this.logger.debug(`Unzip and upload: ${fileName}`);
                  res(true);
                })
                .on('error', (error) => {
                  this.logger.error(`Fail to unzip and upload: ${fileName}`,
                    error);
                  rej(error);
                });
            });
            promises.push(uploadPromise);
          } else {
            this.logger.info(`Unzip skip folder: ${fileName}`);
            entry.autodrain();
          }
        })
        .on('error', reject)
        .on('finish', async () => {
          await Promise.all(promises);
          this.logger.info(
            `Finished unzip ${zipFile.name} to ${targetBucket.name}`);
          resolve(true);
        });
    });
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
    const { options } = this.config;
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
    const {/** @const {!StorageFileConfig} */ source } = this.config;
    const filter = { prefix: source.name };
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
