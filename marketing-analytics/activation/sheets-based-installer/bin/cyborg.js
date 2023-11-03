#!/usr/bin/env node

// Copyright 2023 Google Inc.
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
 * Folder name of Apps Script files.
 * @const{string}
 */
const APPS_SCRIPT_FOLDER = 'apps_script';

/**
 * File name of solutions' configuration, including source path, script Id, etc.
 * @const{string}
 */
const SOLUTIONS_CONFIG_FILE = 'solutions_config.json';

/**
 * Cyborg source code class. This is used to resolve internal module dependency
 * so the deployed files can skip some unused files.
 */
class CyborgSource {

  /**
   * @constructor
   * @param {string} root Source code root path.
   */
  constructor(root) {
    this.root = root;
    // Cyborg internal dependency definition.
    this.dependency = JSON.parse(
      fs.readFileSync(path.join(root, 'dependency.json'), 'utf8'));

    /**
     * An array of the files that are included in the current solution.
     * @const {!Array<string>}
     */
    this.includes = new Set(this.dependency._default);
    /**
     * A map of file path information with file names as the keys, and relative
     * paths to the files as values.
     * @const {Object<string, string>}
     */
    this.files = {};
    /**
     * A map of folders and files in it. The folder names are the keys, and
     * arrays of file names are the values.
     * @const {Object<string, !Array<string>>}
     */
    this.folders = {};
  }

  /** Default load function to initialize `this.files` and `this.folders`.  */
  load() {
    this.loadFolder_(APPS_SCRIPT_FOLDER);
  }

  /**
   * Loads file information into `this.files` and `this.folders`.
   * @param {string} folder
   * @param {string} parent
   * @private
   */
  loadFolder_(folder, parent = '') {
    const prefix = path.join(this.root, parent, folder);
    this.folders[folder] = [];
    fs.readdirSync(prefix).forEach((resource) => {
      this.folders[folder].push(resource);
      const resourceFullPath = path.join(prefix, resource);
      if (fs.lstatSync(resourceFullPath).isDirectory()) {
        this.loadFolder_(resource, path.join(parent, folder));
      } else {
        this.files[resource] = path.join(parent, folder, resource);
      }
    })
  }

  /**
   * Returns an array of all files that current solution needs by solving all
   * dependency requirements.
   * @param {!Array<string>} items An array of files that current solution
   *   includes.
   * @return {!Array<string>}
   */
  resolveDependency(items) {
    items.forEach((item) => {
      if (this.includes.has(item)) {
        // console.log(`Already has ${item}. Skip.`);
        return;
      }
      if (this.files[item]) {
        this.includes.add(item);
      } else if (this.folders[item]) {
        this.resolveDependency(this.folders[item]);
      } else {
        console.error('Can not find resource', item);
      }
      if (this.dependency[item]) {
        this.resolveDependency(this.dependency[item]);
      }
    });
    return Array.from(this.includes).map((file) => this.files[file]).sort();
  }
}
/**
 * The solution based on Cyborg. The solution is all the the configurations that
 * can be used to create a Google Sheets based tool to install the solution.
 */
class CyborgSolution {

  /**
   * @param {string} baseDir Solution root path.
   * @param {{source:string, env: Object<string, string>}} config
   * @param {string} solution Name of the solution.
   * @param {string} target Name of the target (env).
   */
  constructor(baseDir, config, solution, target) {
    this.solution = solution;
    this.target = target;
    // Configurations of the solution to be deployed
    this.config = config;
    // Solution (to be deployed) root path
    this.solutionSource = path.join(baseDir, this.config.source);
    // Destination root path where files are prepared be pushed to Apps Script
    this.destination = path.join(baseDir, `dest/${solution}`);
    // Destination Apps Script root path
    this.destAppsScriptRoot = path.join(this.destination, APPS_SCRIPT_FOLDER);
    // Compressed files root path
    this.compressedDesc = path.join(this.destination, `compressed`)
  }

  /** Clears destination folder. */
  init() {
    console.log(`Start depolying [${this.solution}] to ${this.target}...`);
    fse.emptyDirSync(this.destination);
    console.log('> Clear destination:', this.destination);
  }

  /** Copies Cyborg modules (files) based on Solution's config. */
  copyCyborgLibrary(cyborgLibSource) {
    const cyborgConfigFile = path.join(this.solutionSource, 'cyborg.json');
    let includes, appendedSource;
    if (fs.existsSync(cyborgConfigFile)) {
      const cyborgConfig = fse.readJsonSync(cyborgConfigFile);
      const cyborgSource = new CyborgSource(cyborgLibSource);
      cyborgSource.load();
      includes = cyborgSource.resolveDependency(cyborgConfig.includes);
      appendedSource = cyborgConfig.appendedSource;
    }
    if (includes) {
      console.log(`> Copy ${includes.length} Cyborg files: `, includes);
      includes.forEach((file) => {
        fse.copySync(
          path.join(cyborgLibSource, file), path.join(this.destination, file));
      });
    } else {
      console.log('> Copy Cyborg complete library.');
      fse.copySync(path.join(cyborgLibSource, APPS_SCRIPT_FOLDER),
        this.destAppsScriptRoot);
    }
    if (appendedSource) {
      console.log('>> With appended files:', appendedSource);
      appendedSource.forEach((file) => {
        const resource = path.join(this.destAppsScriptRoot, file);
        fse.copySync(path.join(this.solutionSource, '..', resource),
          path.join(dest, resource));
      });
    }
  }

  /** Copies solution's source codes and configuration files. */
  copyProjectFiles() {
    this.copySource(); // Source code files of this solution
    // Loads Tentacles Api config file
    this.loadJsonFileToAppsScript('config_api.json', 'DEFAULT_TENTACLES_CONFIGS');
    // Loads Sentinel task config file
    this.loadJsonFileToAppsScript('config_task.json', 'DEFAULT_TASK_CONFIGS');
    // Loads sqls in folder 'sql'
    this.loadFilesToAppsScript('sql', 'DEFAULT_SQL_FILES');
  }

  /** Copies source code files of this solution. */
  copySource() {
    console.log('> Copy solution source:', this.solutionSource);
    fse.copySync(path.join(this.solutionSource, APPS_SCRIPT_FOLDER),
      this.destAppsScriptRoot);
  }

  /**
   * Loads JSON content of a specified file as a JSON object into Apps Script.
   * This can help load `config_api.js` or `config_task.js` for existing
   * solutions.
   * @param {string} fileName
   * @param {string} jsonObjectName
   */
  loadJsonFileToAppsScript(fileName, jsonObjectName) {
    const file = path.join(this.solutionSource, fileName);
    if (fs.existsSync(file)) {
      console.log(`> load JSON file: ${file} as ${jsonObjectName}`);
      const jsonString = fs.readFileSync(file).toString();;
      fse.outputFileSync(path.join(this.destAppsScriptRoot, `_${fileName}.js`),
        `const ${jsonObjectName} = ${jsonString}`);
    }
  }

  /**
   * Loads files content of a specified folder as a JSON object into Apps Script.
   * This can help load sql files in the folder `sql`.
   * @param {string} folderName
   * @param {string} jsonObjectName
   */
  loadFilesToAppsScript(folderName, jsonObjectName) {
    const loadFiles = (folder) => {
      if (fse.pathExistsSync(folder)) {
        const files = {};
        fs.readdirSync(folder).forEach((fileName) => {
          const file = path.join(folder, fileName);
          if (fs.lstatSync(file).isDirectory()) {
            const results = loadFiles(file);
            Object.keys(results).forEach((key) => {
              files[`${fileName}/${key}`] = results[key];
            });
          } else {
            files[fileName] = fs.readFileSync(file).toString();
          }
        });
        return files;
      }
    }
    const files = loadFiles(path.join(this.solutionSource, folderName));
    if (files) {
      console.log(`> load folder: ${folderName} as ${jsonObjectName}`);
      fse.outputFileSync(path.join(this.destAppsScriptRoot, `_${folderName}.js`),
        `const ${jsonObjectName} = ${JSON.stringify(files)}`);
    }
  }

  /** Prepares the source files to be deployed or minified. */
  prepareDesc(cyborgLibSource) {
    this.init();
    this.copyCyborgLibrary(cyborgLibSource); // Copy Cyborg lib files
    this.copyProjectFiles(); // Copy all other project files
  }

  /** Minifies Cyborg source codes. */
  compress() {
    // Cyborg JavaScript files will be merged and minified to '_cyborg.min.js'.
    this.compressFilesToSingle(
      '_cyborg.min.js', `${this.destAppsScriptRoot}/*/**/*.js`);
    // Cyborg other files will be copied.
    this.copyFilesToCompress(
      `${this.destAppsScriptRoot}/*/**`, { nodir: true, ignore: '**/*.js' });
    // Project files are under root path.
    this.copyFilesToCompress(`${this.destAppsScriptRoot}/*`, { nodir: true });
  }

  /** Minifies partial Cyborg source codes based on given folders. */
  partiallyCompress(uncompress = process.env.npm_config_uncompress) {
    if (!uncompress) {
      console.log(`No 'uncompress' was set, will compress all Cyborg files`);
      this.compress();
      return;
    }
    const exposeFolders = uncompress.split(',').map((folder) => folder.trim());
    console.log('Will uncompress Cyborg folders end with: ', exposeFolders);
    const folders = globSync(`${this.destAppsScriptRoot}/*/`).sort();
    console.log('Cyborg folders', folders);
    folders.forEach((folder) => {
      const folderName = folder.substring(folder.lastIndexOf('/') + 1);
      if (!exposeFolders.some((name) => folderName.endsWith(name))) {
        // Cyborg JavaScript files will be minified.
        this.compressFilesToSingle(`${folderName}.min.js`, `${folder}/**/*.js`);
        // Cyborg other files will be copied.
        this.copyFilesToCompress(
          `${folder}/**`, { nodir: true, ignore: '**/*.js' });
      } else { // Copy source files for uncompressed folder.
        this.copyFilesToCompress(`${folder}/**`, { nodir: true });
      }
    });
    // Project files are under root path.
    this.copyFilesToCompress(`${this.destAppsScriptRoot}/*`, { nodir: true });
  }

  /** Copies files to the compress folder. */
  copyFilesToCompress(pattern, options) {
    const files = globSync(pattern, options);
    if (files.length === 0) return;
    console.log(`Copy ${files.length} files to compress folder:`, files);
    files.forEach((file) => {
      const relativePath = file.replace(this.destAppsScriptRoot, '');
      fse.copySync(file, path.join(this.compressedDesc, relativePath));
    });
  }

  /** Minifies files to a single one in the compress folder. */
  compressFilesToSingle(fileName, pattern, options = { nodir: true }) {
    const files = globSync(pattern, options).sort();
    if (files.length === 0) return;
    console.log(`Merge ${files.length} files to ${fileName}`, files);
    const contents = files.map((file) => fse.readFileSync(file).toString());
    const minified = UglifyJS.minify(contents.join('\n'));
    fse.outputFileSync(path.join(this.compressedDesc, fileName), minified.code);
  }

  /** Generates '.clasp.json' in destination folder `clasp` to deploy codes. */
  generateClaspJson(rootDir) {
    console.log('> Generate .clasp.json:', rootDir);
    fs.writeFileSync(path.join(rootDir, '.clasp.json'),
      JSON.stringify({
        scriptId: this.config.env[this.target].scriptId,
        rootDir,
      })
    );
  }

  /** Uses `clasp` to push the codes. */
  clashPush(compressed = true) {
    const rootDir = compressed ? this.compressedDesc : this.destAppsScriptRoot;
    this.generateClaspJson(rootDir); // '.clasp.json' file for clasp
    console.log(`> Starting clasp pushing from [${rootDir}]...`);
    execFile('clasp', ['push', '-f', '-P', rootDir],
      (error, data) => {
        if (error) {
          console.error(
            `[ERROR]: Failed to push code to '${this.solution}@${this.target}'`);
          console.error(error);
        }
        console.log(data.toString());
      });
  }

  /** Opens the Apps Script editor page. */
  openScript() {
    console.log(
      `Start opening Apps Script of '${this.solution}@${this.target}'...`);
    const scriptId = this.config.env[this.target].scriptId;
    if (!scriptId) {
      console.error(
        `[ERROR]: Failed to open Apps Script '${this.solution}@${this.target}'`);
      process.exit(-1);
    }
    open(`https://script.google.com/d/${scriptId}/edit`);
  }

  /** Opens the Google Sheets. */
  openSheet() {
    console.log(
      `Start opening Google Sheets of '${this.solution}@${this.target}'...`);
    const sheetId = this.config.env[this.target].parentId;
    if (!sheetId) {
      console.error(
        `[ERROR]: Failed to find Sheet ID for '${this.solution}@${this.target}'`);
      process.exit(-1);
    }
    open(`https://drive.google.com/open?id=${sheetId}`);
  }
}

/**
 * Loads solution configs from the solutionsFile.
 * @param {string} solutionsFile
 * @param {boolean=} allowEmpty
 * @return {Object}
 */
const getSolutions = (solutionsFile, allowEmpty = false) => {
  try {
    return JSON.parse(fs.readFileSync(solutionsFile, 'utf8'));
  } catch (error) {
    if (!allowEmpty) {
      console.error(`[ERROR]: Solutions config is not valid: ${solutionsFile}`);
      console.error(error);
      process.exit(-1);
    } else {
      return {};
    }
  }
}

/**
 * Loads the config for the specified solution and target from command.
 * @param {string} solutionsFile
 * @param {boolean=} allowEmpty
 * @return {{
 *   solution: string,
 *   target: string,
 *   config: Object,
 * }}
 */
const getConfig = (solutionsFile, allowEmpty = false) => {
  // Solution to be deployed
  let solution = process.env.npm_config_solution;
  // Env (dev, test, prod, etc.) to be deployed
  const target = process.env.npm_config_target || 'dev';
  const solutions = getSolutions(solutionsFile, allowEmpty);
  if (!solution && Object.keys(solutions).length === 1) {
    solution = Object.keys(solutions)[0];
  }
  // Configurations of the solution to be deployed
  const config = solutions[solution] || (allowEmpty ? {} : undefined);
  if (!config) {
    console.error(`[ERROR]: Solution[${solution}] doesn't exist.`);
    process.exit(-1);
  }
  if (!config.env || !config.env[target]) {
    if (!allowEmpty) {
      console.error(
        `[ERROR]: Solution[${solution}] doesn't have target [${target}]`);
      process.exit(-1);
    }
  }
  return { solution, target, config };
}

/**
 * Initialize a new Google Sheets for the specified solution and target.
 * @param {string} baseDir
 * @param {string} solutionsFile
 */
const init = (baseDir, solutionsFile) => {
  const solutions = getSolutions(solutionsFile, true);
  const { solution, target, config } = getConfig(solutionsFile, true);
  if (config.env && config.env[target]) {
    console.error(
      `[ERROR]: Solution '${solution}@${target}' exists:`, config.env[target]);
    process.exit(-1);
  }
  if (config.source && process.env.npm_config_source
    && config.source !== process.env.npm_config_source) {
    console.log(
      `[WARN]: Solution[${solution}] has source '${config.source}'. Ingore:`,
      process.env.npm_config_source
    );
  }
  const source = config.source
    ? config.source
    : (process.env.npm_config_source || `./src/${solution}`);
  if (!fs.existsSync(source)) {
    fs.mkdirSync(path.join(source, APPS_SCRIPT_FOLDER), { recursive: true });
  }
  const env = config.env || {};
  console.log(
    `Start creating a new Google Sheets for ${solution}@[${target}]...`);
  execFile('clasp', ['create', '--type', 'sheets', '--title', solution],
    (error, data) => {
      if (error) {
        console.error(
          `[ERROR]: Failed to open Apps Script '${this.solution}@${this.target}'`);
        console.error(error);
        process.exit(-1);
      }
      console.log(data.toString());
      const claspFile = path.join(baseDir, '.clasp.json');
      const { scriptId, parentId } =
        JSON.parse(fs.readFileSync(claspFile, 'utf8'));
      env[target] = { scriptId, parentId: parentId[0] };
      solutions[solution] = { source, env };
      fse.outputFileSync(solutionsFile, JSON.stringify(solutions));
      fse.removeSync(claspFile);
      fse.removeSync(path.join(baseDir, 'appsscript.json'));
      console.log(
        `> Updated config for ${solution}@[${target}]...`);
    });
}

/**
 * Lists solutions configurations.
 * @param {string} solutionsFile
 */
const list = (solutionsFile) => {
  console.log('Start listing solutions from:', solutionsFile, '\n');
  const solutions = getSolutions(solutionsFile);
  Object.keys(solutions).forEach((solution) => {
    const config = solutions[solution];
    console.log(`Solution [${solution}], source:`, config.source);
    Object.keys(config.env).forEach((env) => {
      const { parentId, scriptId } = config.env[env];
      console.log('  └─ [%s] Google Sheets Id: %s', env, parentId);
      console.log('       └─  ScriptId: %s', scriptId);
    });
    console.log();
  });
}

import path from 'path';
import fs from 'fs';
import fse from 'fs-extra';
import open from 'open';
import { execFile } from 'child_process';
import UglifyJS from 'uglify-js';
import { globSync } from 'glob';

// Solution root path
const baseDir = path.resolve();
// Solution configurations file
const solutionsFile = path.join(baseDir, SOLUTIONS_CONFIG_FILE);
// Cyborg library source code path
const cyborgLibSource = path.join(
  path.dirname(fs.realpathSync(process.argv[1])), '../', 'src');
const getCyborgSolution = () => {
  const { solution, target, config } = getConfig(solutionsFile);
  return new CyborgSolution(baseDir, config, solution, target);
}

const argv = process.argv;
const cmd = argv[2];

if (cmd === 'init') {
  init(baseDir, solutionsFile);
} else if (cmd === 'list') {
  list(solutionsFile);
} else if (cmd === 'open' || cmd === 'openScript') {
  getCyborgSolution().openScript();
} else if (cmd === 'openSheet') {
  getCyborgSolution().openSheet();
} else if (cmd === 'deploy') {
  const cyborg = getCyborgSolution();
  cyborg.prepareDesc(cyborgLibSource);
  cyborg.partiallyCompress();
  cyborg.clashPush();
} else if (cmd === 'debug') {
  const cyborg = getCyborgSolution();
  cyborg.prepareDesc(cyborgLibSource);
  cyborg.clashPush(false);
} else {
  console.log(`cyborg: unrecognized command ${cmd}`);
  console.log('usage: npm exec cyborg init --solution=SOLUTION [--src=./src/SOLUTION] [--target=dev]');
  console.log('       npm exec cyborg list');
  console.log('       npm exec cyborg deploy --solution=SOLUTION [--target=ENV] [--uncompress=FOLDERS]');
  console.log('       npm exec cyborg debug --solution=SOLUTION [--target=ENV]');
  console.log('       npm exec cyborg open|openScript --solution=SOLUTION [--target=ENV]');
  console.log('       npm exec cyborg openSheet --solution=SOLUTION [--target=ENV]');
}
