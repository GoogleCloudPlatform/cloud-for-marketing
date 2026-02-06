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

/**
 * @fileoverview Google Cloud Storage (GCS) file facilitate class.
 */

'use strict';

const {Storage, Bucket, File, StorageOptions} = require(
    '@google-cloud/storage');

/** Default file size for split. */
const DEFAULT_SPLIT_SIZE = 999 * 1000 * 1000;
/** Line breaker for GCS files */
const LINE_BREAKER = '\n';

/**
 * Cloud Storage Utility Class. There are two main usages:
 * 1. Appends a 'header' string to a given GCS file to create a new file;
 * 2. Splits a big file into a couple of files with the size no exceeded the
 * given split size and keep not breaking the lines.
 */
class StorageFile {
  /**
   * Initializes StorageFile.
   * @param {string} bucketName GCS bucket name.
   * @param {string} fileName GCS file name.
   * @param {StorageOptions?} options Options to initiate cloud storage.
   */
  constructor(bucketName, fileName, options) {
    /** @type {Bucket} */
    this.bucket = (new Storage(options)).bucket(bucketName);
    this.fileName = fileName;
    /** @type {File} */
    this.file = this.bucket.file(fileName);
  };

  /**
   * Returns the Google Cloud Storage File Object.
   * @return {!File}
   */
  getFile() {
    return this.file;
  }

  /**
   * Gets the file size.
   * @return {!Promise<number>}
   */
  async getFileSize() {
    const fileResponse = await this.file.get();
    return parseInt(fileResponse[1].size, 10);
  };

  /**
   * Lists the files with the given prefix.
   * @param {string=} prefix The file name prefix.
   * @return {!Promise<!Array<string>>} Array of file names.
   */
  async listFiles(prefix = this.fileName) {
    const options = {prefix: prefix, delimiter: '/'};
    const [files] = await this.bucket.getFiles(options);
    return files.map((file) => file.name);
  }

  /**
   * Loads the content of GCS file with given start and end positions.
   *
   * @param {number=} start Start position, default 0.
   * @param {?number} end End position, default is the end of the file.
   * @return {!Promise<string>} contents File content between the start and end.
   */
  async loadContent(start = 0, end) {
    if (start < 0) {
      console.log(`GCS load 'start' before 0 [${start}], move it to 0.`);
      start = 0;
    }
    if (end < start) {
      console.log(`GCS load for [${start}, ${end}], returns empty string.`);
      return '';
    }
    const option = { start, end };
    const stream = this.file.createReadStream(option);
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', (chunk) => void chunks.push(chunk));
      stream.on('end', () => {
        console.log(`Get [${this.fileName}] from ${start} to ${end}`);
        resolve(Buffer.concat(chunks).toString());
      });
      stream.on('error', (error) => void reject(error));
    });
  };

  /**
   * Gets the index of last line breaker in the given range of a file.
   * Throws an error if there is no line break between start and end.
   * @param {number} start The start point this round split.
   * @param {number} end The most end point this round split based on split
   *     size.
   * @param {number=} checkPoint point for the latest line breaker
   * @return {!Promise<number>} The position of the last line breaker between
   *     the start and end points.
   */
  async getLastLineBreaker(start, end, checkPoint = -1) {
    /**
     * How many characters to look back to find a possible line breaker. If no
     * link break in this range, it will extend to find the last one.
     */
    const possibleLineBreakRange = 1000;
    if (checkPoint < 0 || checkPoint > end) {
      checkPoint = Math.max(start, end - possibleLineBreakRange + 1);
    }
    const content = await this.loadContent(checkPoint, end);
    let i = 0;
    while (content.charCodeAt(i) === 0xFFFD) i++;
    const cleanedContent = i > 0 ? content.slice(i) : content;
    const index = Buffer.from(cleanedContent).lastIndexOf(LINE_BREAKER);
    if (index >= 0) {
      return checkPoint + i + index;
    }
    if (checkPoint > start) {
      return this.getLastLineBreaker(
          start, end,
          Math.max(start, checkPoint - possibleLineBreakRange));
    } else {
      console.error(`Filename ${this.fileName}, from ${start} to ${
          end}. Error: there is no line breaker in ${content}.`);
      throw new Error('No line breaker found in the line');
    }
  }

  /**
   * Generates a split plan(array of start-end pair values) for a large file.
   * It will avoid breaking the line when calculate how to split the file by the
   * given splitSize.
   * @param {number} fileSize The size of whole file.
   * @param {number} splitSize The split size.
   * @param {number=} index The start point of this round split.
   * @return {!Promise<!Array<!Array<number,number>>>}
   */
  async getSplitRanges(fileSize, splitSize, index = 0) {
    if (index + splitSize >= fileSize) {
      return [[index, fileSize - 1]];
    } else {
      const end = index + splitSize - 1;
      const realEnd = await this.getLastLineBreaker(index, end);
      const piece = [[index, realEnd]];
      const splits = await this.getSplitRanges(fileSize, splitSize, realEnd + 1);
      return piece.concat(splits);
    }
  }

  /**
   * Creates a new file based on the start and end points of a given file.
   * @param {number} start Start point (included).
   * @param {number} end End point (included).
   * @param {string} croppedFileName New cropped file name.
   * @return {!Promise<string>} Cropped file name.
   * @private
   */
  copyRangeToFile(start, end, croppedFileName) {
    const outputFile = this.bucket.file(croppedFileName);
    return new Promise((resolve) => {
      this.file.createReadStream({ start, end, })
        .pipe(outputFile.createWriteStream())
        .on('finish', async () => {
          const [{ contentType }] = await this.file.getMetadata();
          const [file] = await outputFile.setMetadata({ contentType });
          resolve(file.name);
        });
    });
  }

  /**
   * Creates a new GCS file with the header (a string) ahead of the given GCS
   * source file. In some cases, the source file has no valid header (title)
   * line. For example, when use BigQuery to export CSV file, the column names
   * can't contain colon. However, for GA Data Import, the colon is required in
   * GA Data Import format. Use this function to put the headline ahead of a
   * file and create a new file to upload.
   *
   * @param {string} header Header line.
   * @param {string=} sourceName Source file name, default value is the file
   *     name of this instance.
   * @param {string=} outputName File name for output.
   * @return {!Promise<string>} Output file name.
   */
  async addHeader(
      header, sourceName = this.fileName,
      outputName = sourceName + '_w_header') {
    const headerFile = this.bucket.file('_header_' + (new Date()).getTime());
    if (!header.endsWith(LINE_BREAKER)) header = header + LINE_BREAKER;
    await headerFile.save(header);
    const sourceFile = this.bucket.file(sourceName);
    const [{ contentType }] = await sourceFile.getMetadata();
    const [outputFile] = await this.bucket.combine(
      [headerFile, sourceFile], this.bucket.file(outputName));
    const [file] = await outputFile.setMetadata({ contentType });
    await headerFile.delete();
    return file.name;
  }

  /**
   * Splits a GCS file into multiple files based on the given maximum file size.
   * Note: This split won't break lines. So the sizes of output files may be
   * slightly less than the given maximum size.
   *
   * @param {number=} splitSize The size of content for new files after split.
   *     Default value DEFAULT_SPLIT_SIZE.
   * @return {!Promise<!Array<string>>} The filenames of the output files.
   */
  async split(splitSize = DEFAULT_SPLIT_SIZE) {
    const size = await this.getFileSize();
    if (size <= splitSize) {  // No need to split.
      return [this.fileName];
    }
    // Trying to split the big file.
    console.log(`Get file size: ${size}, split size: ${splitSize}.`);
    const splitRanges = await this.getSplitRanges(size, splitSize);
    return Promise.all(splitRanges.map(([start, end], index) => {
      const newFile = `${this.fileName}-${index}-of-${splitRanges.length}`;
      return this.copyRangeToFile(start, end, newFile);
    }));
  }

  /**
   * Returns a new instance of this class. Using this function to replace
   * constructor to be more friendly to unit tests.
   * @param {string} bucketName GCS bucket name.
   * @param {string} fileName GCS file name.
   * @param {StorageOptions?} options Options to initiate cloud storage.
   * @return {!StorageFile}
   * @static
   */
  static getInstance(bucketName, fileName, options) {
    return new StorageFile(bucketName, fileName, options);
  };
}

module.exports = {
  StorageFile,
  LINE_BREAKER,
  DEFAULT_SPLIT_SIZE,
};
