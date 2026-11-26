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
/**
 * @fileoverview
 * @suppress {lintChecks, moduleLoad}
 */

import path from 'path';
import fs from 'fs';
import fse from 'fs-extra';
import open from 'open';
import {execFile, execFileSync} from 'child_process';
import UglifyJS from 'uglify-js';
import {globSync} from 'glob';

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
 * so the pushed files can skip some unused files.
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
      fs.readFileSync(path.join(root, 'dependency.json'), 'utf8'),
    );

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
    /**
     * Dictionary mapping declared top-level symbols (classes, functions) to
     * their Cyborg library filename.
     * @const {!Map<string, string>}
     */
    this.symbolMap = new Map();
    this.templateMap = new Map();
  }

  /** Default load function to initialize `this.files` and `this.folders`. */
  load() {
    this.loadFolder_(APPS_SCRIPT_FOLDER);
    this.buildSymbolRegistry_();
  }

  /**
   * Scans all loaded Cyborg source files to build a mapping from exported
   * symbol names (classes, functions, constants) to filename.
   * @private
   */
  buildSymbolRegistry_() {
    const stripComments = code =>
      code.replace(
        /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)|\/\*[\s\S]*?\*\/|\/\/.*/g,
        (match, str) => (str ? str : ''),
      );

    Object.entries(this.files).forEach(([fileName, relativePath]) => {
      this.symbolMap.set(fileName, fileName);
      const fullPath = path.join(this.root, relativePath);
      if (!fs.existsSync(fullPath)) return;
      const rawCode = fs.readFileSync(fullPath, 'utf8');
      const cleanCode = stripComments(rawCode);

      // Match top-level classes: class ClassName
      const classMatches = cleanCode.matchAll(/^class\s+([A-Za-z0-9_]+)/gm);
      for (const match of classMatches) {
        this.symbolMap.set(match[1], fileName);
      }

      // Match top-level functions: function functionName(
      const funcMatches = cleanCode.matchAll(
        /^function\s+([A-Za-z0-9_]+)\s*\(/gm,
      );
      for (const match of funcMatches) {
        this.symbolMap.set(match[1], fileName);
      }

      // Match top-level exported constants / namespaces: const CONST_NAME = ...
      // Only register uppercase constants (e.g. UPPER_SNAKE or PascalCase) or
      // namespaces (gcloud, sentinel, tentacles)
      const varMatches = cleanCode.matchAll(
        /^(?:const|let|var)\s+([A-Za-z0-9_]+)\s*=/gm,
      );
      for (const match of varMatches) {
        const sym = match[1];
        if (
          /^[A-Z]/.test(sym) ||
          [
            'gcloud',
            'sentinel',
            'tentacles',
            'enhancedSheet',
            'MojoFunctions',
          ].includes(sym)
        ) {
          this.symbolMap.set(sym, fileName);
        }
      }

      // Match MOJO_CONFIG_TEMPLATE.templateName =
      const templateMatches = cleanCode.matchAll(
        /MOJO_CONFIG_TEMPLATE\.([A-Za-z0-9_]+)\s*=/gm,
      );
      for (const match of templateMatches) {
        this.templateMap.set(match[1], fileName);
      }

      // Match MojoFunctions namespace function definitions:
      // MojoFunctions.functionName =
      const mojoFuncMatches = cleanCode.matchAll(
        /^MojoFunctions\.([A-Za-z0-9_]+)\s*=/gm,
      );
      for (const match of mojoFuncMatches) {
        this.symbolMap.set('MojoFunctions.' + match[1], fileName);
      }

      // Match MojoTemplateRegistry.register('templateName'
      const registryMatches = cleanCode.matchAll(
        /MojoTemplateRegistry\.register\s*\(\s*['"]([A-Za-z0-9_]+)['"]/gm,
      );
      for (const match of registryMatches) {
        this.templateMap.set(match[1], fileName);
      }
    });
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
    fs.readdirSync(prefix).forEach(resource => {
      this.folders[folder].push(resource);
      const resourceFullPath = path.join(prefix, resource);
      if (fs.lstatSync(resourceFullPath).isDirectory()) {
        this.loadFolder_(resource, path.join(parent, folder));
      } else {
        this.files[resource] = path.join(parent, folder, resource);
      }
    });
  }

  /**
   * Returns an array of all files that current solution needs by solving all
   * dependency requirements, with full trace diagnostics.
   * @param {!Array<string>} roots An array of files that current solution
   *     includes.
   * @param {!Map<string, !Array<{solutionFile: string, symbol: string, line:
   *     number}>>} originMap
   * @return {{includes: !Array<string>, traceReport: string}}
   */
  resolveDependencyWithTrace(roots, originMap = new Map()) {
    const visited = new Set();

    const resolveItemTree = (item, reason) => {
      if (visited.has(item)) return null;
      visited.add(item);

      const relPath = this.files[item] || item;
      const node = {
        item,
        relPath,
        reason,
        children: [],
      };

      const directDeps = [];
      if (this.folders[item]) {
        this.folders[item].forEach(f => directDeps.push(f));
      }
      if (this.dependency[item]) {
        this.dependency[item].forEach(dep => directDeps.push(dep));
      }

      directDeps.forEach(dep => {
        if (!visited.has(dep)) {
          const child = resolveItemTree(dep, `required by ${item}`);
          if (child) node.children.push(child);
        }
      });

      return node;
    };

    // 1. Resolve default base files
    const defaultNodes = [];
    (this.dependency._default || []).forEach(item => {
      const node = resolveItemTree(item, 'Base framework component (_default)');
      if (node) defaultNodes.push(node);
    });

    // 2. Resolve roots from solution
    const rootNodes = [];
    roots.forEach(item => {
      const origins = originMap.get(item);
      const reason =
        origins && origins.length > 0
          ? `Inferred via '${origins[0].symbol}' in ${path.basename(
              origins[0].solutionFile,
            )}`
          : `Explicit root`;
      const node = resolveItemTree(item, reason);
      if (node) rootNodes.push(node);
    });

    const formatTree = (node, prefix = '') => {
      let out = '';
      const len = node.children.length;
      node.children.forEach((child, i) => {
        const isLast = i === len - 1;
        const branch = isLast ? '└── ' : '├── ';
        const childPrefix = prefix + (isLast ? '    ' : '│   ');
        out += `${prefix}${branch}${child.relPath}  [${child.reason}]\n`;
        out += formatTree(child, childPrefix);
      });
      return out;
    };

    let report =
      '================================================================================\n';
    report +=
      '                 CYBORG DEPENDENCY RESOLUTION & TRACE REPORT                    \n';
    report +=
      '================================================================================\n\n';

    if (originMap.size > 0) {
      report += '1. DIRECT REFERENCES INFERRED FROM SOLUTION SOURCE:\n';
      originMap.forEach((origins, file) => {
        const targetPath = this.files[file] || file;
        report += `   • ${targetPath}:\n`;
        origins.forEach(o => {
          report += `       - Symbol '${o.symbol}' referenced in ${path.basename(
            o.solutionFile,
          )}:L${o.line}\n`;
        });
      });
      report += '\n';
    }

    report += '2. DEPENDENCY GRAPH & CHAIN BREAKDOWN:\n';
    report += '   ├── [Base Framework Components (_default)]\n';
    defaultNodes.forEach(node => {
      report += `   │   ├── ${node.relPath}\n`;
      report += formatTree(node, '   │   │   ');
    });

    report += '   └── [Solution Inferred & Custom Components]\n';
    rootNodes.forEach(node => {
      report += `       ├── ${node.relPath}  [${node.reason}]\n`;
      report += formatTree(node, '       │   ');
    });

    const finalFiles = Array.from(visited)
      .map(f => this.files[f])
      .filter(Boolean)
      .sort();

    report += '\n3. SUMMARY:\n';
    report += `   • Total Cyborg files included: ${finalFiles.length}\n`;
    report += `   • Files list:\n`;
    finalFiles.forEach(f => {
      report += `       - ${f}\n`;
    });
    report +=
      '================================================================================\n';

    return {includes: finalFiles, traceReport: report};
  }

  /**
   * Returns an array of all files that current solution needs by solving all
   * dependency requirements.
   * @param {!Array<string>} items An array of files that current solution
   *     includes.
   * @return {!Array<string>}
   */
  resolveDependency(items) {
    const {includes} = this.resolveDependencyWithTrace(items);
    return includes;
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
   * @param {string} uncompressArg Names of uncompressed folders.
   */
  constructor(baseDir, config, solution, target, uncompressArg) {
    this.solution = solution;
    this.target = target;
    // Configurations of the solution to be managed.
    this.config = config;
    // Solution root path
    this.solutionSource = path.join(baseDir, this.config.source);
    // Destination root path where files are prepared be pushed to Apps Script
    this.destination = path.join(baseDir, `dest/${solution}`);
    // Destination Apps Script root path
    this.destAppsScriptRoot = path.join(this.destination, APPS_SCRIPT_FOLDER);
    // Compressed files root path
    this.compressedDesc = path.join(this.destination, `compressed`);
    this.uncompressArg = uncompressArg;
  }

  /** Clears destination folder. */
  init() {
    fse.emptyDirSync(this.destination);
    console.log('> Clear destination:', this.destination);
  }

  /**
   * Scans project source files to infer required Cyborg library components
   * and tracks source origin locations (file, symbol, line).
   * @param {!CyborgSource} cyborgSource Loaded Cyborg source registry.
   * @return {{
   *   inferredRoots: !Array<string>,
   *   originMap: !Map<string, !Array<{solutionFile: string, symbol: string,
   * line: number}>>
   * }}
   * @private
   */
  inferProjectIncludes_(cyborgSource) {
    const inferredRoots = new Set();
    const originMap = new Map(); // key: target filename, value:
    // Array<{solutionFile, symbol, line}>

    const scanDir = dir => {
      if (!fs.existsSync(dir)) return;
      fs.readdirSync(dir, {withFileTypes: true}).forEach(entry => {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanDir(fullPath);
        } else if (entry.name.endsWith('.js') || entry.name.endsWith('.html')) {
          const rawCode = fs.readFileSync(fullPath, 'utf8');

          // Phase 1: Strip ALL comments globally but preserve newlines to keep
          // line numbers intact. This safely extracts all multi-line blocks and
          // inline comments.
          const codeWithoutComments = rawCode.replace(
            /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)|\/\*[\s\S]*?\*\/|\/\/.*/g,
            (match, str) => {
              if (str) return str; // keep strings intact
              // For comments, return exactly the same number of newlines they
              // occupied
              return match.replace(/[^\n]/g, '');
            },
          );

          const lines = codeWithoutComments.split('\n');

          lines.forEach((lineText, lineIdx) => {
            // Phase 2: lineText has no comments, but retains strings. Safely
            // grab templates here.
            const tMatches = lineText.matchAll(
              /template\s*:\s*['"]([A-Za-z0-9_]+)['"]/g,
            );
            for (const match of tMatches) {
              const tmpl = match[1];
              if (cyborgSource.templateMap.has(tmpl)) {
                const targetFile = cyborgSource.templateMap.get(tmpl);
                inferredRoots.add(targetFile);
                if (!originMap.has(targetFile)) {
                  originMap.set(targetFile, []);
                }
                originMap.get(targetFile).push({
                  solutionFile: fullPath,
                  symbol: `template:${tmpl}`,
                  line: lineIdx + 1,
                });
              }
            }

            // Phase 2.5: Match MojoFunctions namespace method calls:
            // MojoFunctions.xxx
            const mojoCalls = lineText.matchAll(
              /\bMojoFunctions\.([A-Za-z0-9_]+)\b/g,
            );
            for (const match of mojoCalls) {
              const sym = 'MojoFunctions.' + match[1];
              if (cyborgSource.symbolMap.has(sym)) {
                const targetFile = cyborgSource.symbolMap.get(sym);
                inferredRoots.add(targetFile);
                if (!originMap.has(targetFile)) {
                  originMap.set(targetFile, []);
                }
                const existing = originMap.get(targetFile);
                if (
                  !existing.some(
                    e => e.solutionFile === fullPath && e.symbol === sym,
                  )
                ) {
                  existing.push({
                    solutionFile: fullPath,
                    symbol: sym,
                    line: lineIdx + 1,
                  });
                }
              }
            }

            // Phase 3: Strip strings out of the line so they don't trigger
            // false positive identifiers.
            const cleanLine = lineText.replace(
              /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)/g,
              '',
            );

            const idMatches = cleanLine.matchAll(
              /\b([A-Za-z_][A-Za-z0-9_]*)\b/g,
            );
            for (const match of idMatches) {
              const sym = match[1];
              if (cyborgSource.symbolMap.has(sym)) {
                const targetFile = cyborgSource.symbolMap.get(sym);
                inferredRoots.add(targetFile);
                if (!originMap.has(targetFile)) {
                  originMap.set(targetFile, []);
                }
                const existing = originMap.get(targetFile);
                if (
                  !existing.some(
                    e => e.solutionFile === fullPath && e.symbol === sym,
                  )
                ) {
                  existing.push({
                    solutionFile: fullPath,
                    symbol: sym,
                    line: lineIdx + 1,
                  });
                }
              }
            }
          });
        }
      });
    };

    scanDir(this.solutionSource);
    return {inferredRoots: Array.from(inferredRoots), originMap};
  }

  /**
   * Copies Cyborg modules (files) based on Solution's config or auto-inferred
   * dependency graph.
   */
  copyCyborgLibrary(cyborgLibSource) {
    const cyborgConfigFile = path.join(this.solutionSource, 'cyborg.json');
    let explicitIncludes = [];
    let appendedSource;
    const cyborgSource = new CyborgSource(cyborgLibSource);
    cyborgSource.load();

    if (fs.existsSync(cyborgConfigFile)) {
      const cyborgConfig = fse.readJsonSync(cyborgConfigFile);
      explicitIncludes = cyborgConfig.includes || [];
      appendedSource = cyborgConfig.appendedSource;
    }

    const {inferredRoots, originMap} = this.inferProjectIncludes_(cyborgSource);
    const combinedRoots = Array.from(
      new Set([...explicitIncludes, ...inferredRoots]),
    );

    const {includes, traceReport} = cyborgSource.resolveDependencyWithTrace(
      combinedRoots,
      originMap,
    );
    this.includedCyborgFiles = includes || [];

    // Save dependency trace report for immediate developer inspection
    const traceReportPath = path.join(
      this.destination,
      '_dependency_trace.txt',
    );
    fse.outputFileSync(traceReportPath, traceReport);
    console.log(traceReport);

    if (includes && includes.length > 0) {
      console.log(`> Copy ${includes.length} Cyborg files: `, includes);
      includes.forEach(file => {
        fse.copySync(
          path.join(cyborgLibSource, file),
          path.join(this.destination, file),
        );
      });
    } else {
      console.log('> Copy Cyborg complete library.');
      fse.copySync(
        path.join(cyborgLibSource, APPS_SCRIPT_FOLDER),
        this.destAppsScriptRoot,
      );
    }
    if (appendedSource) {
      console.log('>> With appended files:', appendedSource);
      appendedSource.forEach(file => {
        const resource = path.join(this.destAppsScriptRoot, file);
        fse.copySync(
          path.join(this.solutionSource, '..', resource),
          path.join(dest, resource),
        );
      });
    }
  }

  /** Copies solution's source codes and configuration files. */
  copyProjectFiles() {
    this.copySource(); // Source code files of this solution
    // Loads env configuration
    this.loadJsonFileToAppsScript(`${this.target}.json`, 'DEFAULT_ENV');
    // Loads Tentacles Api config file
    this.loadJsonFileToAppsScript('config_api.json', 'DEFAULT_API_CONFIGS');
    // Loads Sentinel task config file
    this.loadJsonFileToAppsScript('config_task.json', 'DEFAULT_TASK_CONFIGS');
    // Loads sqls in folder 'sql'
    this.loadFilesToAppsScript('sql', 'DEFAULT_SQL_FILES');
  }

  /** Copies source code files of this solution. */
  copySource() {
    console.log('> Copy solution source:', this.solutionSource);
    fse.copySync(
      path.join(this.solutionSource, APPS_SCRIPT_FOLDER),
      this.destAppsScriptRoot,
    );
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
      const jsonString = fs.readFileSync(file).toString();
      fse.outputFileSync(
        path.join(this.destAppsScriptRoot, `_${fileName}.js`),
        `const ${jsonObjectName} = ${jsonString}`,
      );
    }
  }

  /**
   * Loads files content of a specified folder as a JSON object into Apps
   * Script. This can help load sql files in the folder `sql`.
   * @param {string} folderName
   * @param {string} jsonObjectName
   */
  loadFilesToAppsScript(folderName, jsonObjectName) {
    const loadFiles = folder => {
      if (fse.pathExistsSync(folder)) {
        const files = {};
        fs.readdirSync(folder).forEach(fileName => {
          const file = path.join(folder, fileName);
          if (fs.lstatSync(file).isDirectory()) {
            const results = loadFiles(file);
            Object.keys(results).forEach(key => {
              files[`${fileName}/${key}`] = results[key];
            });
          } else {
            files[fileName] = fs.readFileSync(file).toString();
          }
        });
        return files;
      }
    };
    const files = loadFiles(path.join(this.solutionSource, folderName));
    if (files) {
      console.log(`> load folder: ${folderName} as ${jsonObjectName}`);
      fse.outputFileSync(
        path.join(this.destAppsScriptRoot, `_${folderName}.js`),
        `const ${jsonObjectName} = ${JSON.stringify(files)}`,
      );
    }
  }

  /**
   * Dynamically inspects API wrapper classes in `1_gcp/` and `3_api/` to
   * extract their `this.name`, `this.api`, `this.permissions`,
   * `this.adminRole`, and `this.runtimeRoles` descriptors, and service lists
   * from `service_usage.js`.
   * @param {string} cyborgLibSource Root of Cyborg library (e.g. cyborg/src).
   * @return {{
   *   classMap: !Map<string, {name: string, api: string, permissions:
   * !Array<string>, adminRole: string, runtimeRoles: !Array<string>}>,
   *   serviceListMap: !Map<string, string>
   * }}
   * @private
   */
  loadApiRegistry_(cyborgLibSource) {
    const classMap = new Map();

    const apiFolders = [
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '1_gcp'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '3_api'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '4_artifact'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '2_base'),
    ];
    if (this.solutionSource && fs.existsSync(this.solutionSource)) {
      apiFolders.push(this.solutionSource);
      const solAppsScript = path.join(this.solutionSource, APPS_SCRIPT_FOLDER);
      if (fs.existsSync(solAppsScript)) apiFolders.push(solAppsScript);
    }

    apiFolders.forEach(folder => {
      if (!fs.existsSync(folder)) return;
      fs.readdirSync(folder).forEach(fileName => {
        if (!fileName.endsWith('.js')) return;
        const filePath = path.join(folder, fileName);
        const rawContent = fs.readFileSync(filePath, 'utf8');
        const content = this.stripComments_(rawContent);

        // Extract class name
        const classMatch = content.match(/\bclass\s+([A-Za-z0-9_]+)/);
        if (classMatch) {
          const className = classMatch[1];
          const nameMatch = content.match(/this\.name\s*=\s*['"]([^'"]+)['"]/);
          const apiMatch = content.match(/this\.api\s*=\s*['"]([^'"]+)['"]/);

          // Extract this.permissions array
          const permMatch = content.match(
            /this\.permissions\s*=\s*\[([\s\S]*?)\];/,
          );
          const permissions = permMatch
            ? Array.from(permMatch[1].matchAll(/['"]([^'"]+)['"]/g)).map(
                m => m[1],
              )
            : [];

          // Extract this.runtimeRoles array (backward-compatible with
          // this.roles)
          const roleMatch = content.match(
            /this\.(?:runtimeRoles|roles)\s*=\s*\[([\s\S]*?)\];/,
          );
          const runtimeRoles = roleMatch
            ? Array.from(roleMatch[1].matchAll(/['"]([^'"]+)['"]/g)).map(
                m => m[1],
              )
            : [];

          // Extract this.adminRole string
          const adminRoleMatch = content.match(
            /this\.adminRole\s*=\s*['"]([^'"]+)['"]/,
          );
          const adminRole = adminRoleMatch ? adminRoleMatch[1] : 'Editor';

          // Extract this.companionApis array: [{ name: '...', api: '...' },
          // ...] or ['api1', 'api2']
          const companionMatch = content.match(
            /this\.companionApis\s*=\s*\[([\s\S]*?)\];/,
          );
          const companionApis = [];
          if (companionMatch) {
            const block = companionMatch[1];
            const objMatches = block.matchAll(
              /\{[^{}]*?(?:name\s*:\s*['"]([^'"]+)['"][^{}]*?api\s*:\s*['"]([^'"]+)['"]|api\s*:\s*['"]([^'"]+)['"][^{}]*?name\s*:\s*['"]([^'"]+)['"])[^{}]*?\}/g,
            );
            for (const m of objMatches) {
              const name = m[1] || m[4];
              const api = m[2] || m[3];
              if (api) {
                companionApis.push({name: name || api, api});
              }
            }
            if (companionApis.length === 0) {
              const strMatches = block.matchAll(/['"]([^'"]+)['"]/g);
              for (const sm of strMatches) {
                companionApis.push({name: sm[1], api: sm[1]});
              }
            }
          }

          // Extract getScope() string if defined
          const scopeMatch = content.match(
            /\bgetScope\s*\(\s*\)\s*\{\s*return\s*['"]([^'"]+)['"]/,
          );
          const scope = scopeMatch ? scopeMatch[1] : '';

          if (
            apiMatch ||
            permissions.length > 0 ||
            runtimeRoles.length > 0 ||
            companionApis.length > 0 ||
            scope
          ) {
            const api = apiMatch ? apiMatch[1] : '';
            const name = nameMatch ? nameMatch[1] : api || className;
            classMap.set(className, {
              name,
              api,
              permissions,
              adminRole,
              runtimeRoles,
              companionApis,
              scope,
              fileName,
            });
          }
        }
      });
    });

    return classMap;
  }

  /**
   * Strips single-line and multi-line comments from JavaScript source code
   * while preserving string literals (including URLs and template strings).
   * @param {string} code JavaScript source code.
   * @return {string} Code with comments removed.
   * @private
   */
  stripComments_(code) {
    return code.replace(
      /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)|\/\*[\s\S]*?\*\/|\/\/.*/g,
      (match, str) => (str ? str : ''),
    );
  }

  /**
   * Extracts content of a balanced curly-bracket block `{ ... }` starting from
   * startIndex.
   * @param {string} source Source code.
   * @param {number} startIndex Position to start looking for opening brace.
   * @return {string} Full text of the braced block including braces.
   * @private
   */
  extractBracedBlock_(source, startIndex) {
    const openBrace = source.indexOf('{', startIndex);
    if (openBrace === -1) return '';
    let depth = 1;
    let i = openBrace + 1;
    while (i < source.length && depth > 0) {
      const char = source[i];
      if (char === '{') depth++;
      else if (char === '}') depth--;
      i++;
    }
    return source.slice(openBrace, i);
  }

  /**
   * Dynamically traces MOJO configuration from `this.solutionSource` files
   * through template definitions, check/enable functions, and instantiated
   * GCP/External API classes to produce
   * `_autogen_config.js` (DEFAULT_API, DEFAULT_PERMISSIONS) and
   * `appsscript.json` (timeZone, oauthScopes, webapp) in the solution's source
   * apps_script folder.
   * @param {string} cyborgLibSource Root of Cyborg library (e.g. cyborg/src).
   */
  generateDefaultConfigs(cyborgLibSource) {
    if (!fs.existsSync(this.solutionSource)) return;

    // 1. Load API Class registry (ClassName -> {name, api, permissions,
    // adminRole, roles}) from Cyborg library
    const classMap = this.loadApiRegistry_(cyborgLibSource);

    // 2. Scan ONLY solutionSource JS files to find solution codes and MOJO
    // config
    const solutionJsFiles = [];
    const scanSolutionDir = dir => {
      if (!fs.existsSync(dir)) return;
      fs.readdirSync(dir, {withFileTypes: true}).forEach(entry => {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanSolutionDir(fullPath);
        } else if (entry.name.endsWith('.js')) {
          solutionJsFiles.push(fullPath);
        }
      });
    };
    scanSolutionDir(this.solutionSource);
    if (solutionJsFiles.length === 0) return;

    let solutionCombinedCode = '';
    solutionJsFiles.forEach(file => {
      const rawCode = fs.readFileSync(file, 'utf8');
      solutionCombinedCode += '\n' + this.stripComments_(rawCode);
    });

    // 3. Find MOJO config definitions and solution code

    // 4. Load template definitions (from 5_sheet_element, 6_sheet, and
    // solution's own extensions)
    let allTemplateSource = '';
    const templateFolders = [
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '4_mojo_template'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '5_sheet_element'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '6_sheet'),
    ];
    templateFolders.forEach(folder => {
      if (fs.existsSync(folder)) {
        fs.readdirSync(folder).forEach(file => {
          if (file.endsWith('.js')) {
            allTemplateSource +=
              '\n' +
              this.stripComments_(
                fs.readFileSync(path.join(folder, file), 'utf8'),
              );
          }
        });
      }
    });
    allTemplateSource += '\n' + solutionCombinedCode;

    // Load function source bodies (from 4_artifact, 2_base, and solution
    // source)
    let allFunctionSource = '';
    const functionFolders = [
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '4_artifact'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '4_mojo_template'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '2_base'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '5_sheet_element'),
      path.join(cyborgLibSource, APPS_SCRIPT_FOLDER, '6_sheet'),
    ];
    functionFolders.forEach(folder => {
      if (fs.existsSync(folder)) {
        fs.readdirSync(folder).forEach(file => {
          if (file.endsWith('.js')) {
            allFunctionSource +=
              '\n' +
              this.stripComments_(
                fs.readFileSync(path.join(folder, file), 'utf8'),
              );
          }
        });
      }
    });
    allFunctionSource += '\n' + solutionCombinedCode;

    const detectedApis = new Map(); // key: api endpoint, value: display name
    const detectedPermissions = new Set(); // Pre-flight caller IAM permissions
    const permissionToRoleMap = new Map(); // Permission -> Predefined Admin/Editor Role
    const detectedRuntimeRoles = new Set(); // Runtime Service Account IAM roles

    const addApi = (name, api) => {
      if (!api) return;
      if (!detectedApis.has(api)) {
        detectedApis.set(api, name || api);
      }
    };

    const addClassMetadata = cls => {
      if (!classMap.has(cls)) return;
      const desc = classMap.get(cls);
      if (desc.name && desc.api) addApi(desc.name, desc.api);
      if (desc.companionApis && Array.isArray(desc.companionApis)) {
        desc.companionApis.forEach(comp => {
          if (comp.name && comp.api) addApi(comp.name, comp.api);
        });
      }
      if (desc.permissions) {
        desc.permissions.forEach(p => {
          detectedPermissions.add(p);
          if (desc.adminRole) {
            permissionToRoleMap.set(p, desc.adminRole);
          }
        });
      }
      if (desc.runtimeRoles) {
        desc.runtimeRoles.forEach(r => detectedRuntimeRoles.add(r));
      }
    };

    // Resolves a function name to all API classes instantiated inside its code
    // body and called helpers
    const functionClassCache = new Map();
    const resolveFunctionClasses = (fnName, visited = new Set()) => {
      if (!fnName) return [];
      const cleanFnName = fnName.replace(
        /^(?:gcloud|sentinel|tentacles|MojoFunctions)\./,
        '',
      );
      if (visited.has(cleanFnName)) return [];
      visited.add(cleanFnName);

      if (functionClassCache.has(cleanFnName)) {
        return functionClassCache.get(cleanFnName);
      }

      // Search for function declaration: gcloud.fnName = ... or function
      // fnName(...)
      const fnRegex = new RegExp(
        `(?:(?:gcloud|sentinel|tentacles)\\.${cleanFnName}|${
          cleanFnName
        })\\s*=\\s*(?:async\\s+)?(?:function\\s*\\([^)]*\\)|\\([^)]*\\)|\\w+)\\s*=>?|function\\s+${
          cleanFnName
        }\\s*\\(`,
        'g',
      );
      const match = fnRegex.exec(allFunctionSource);
      const classes = new Set();
      if (match) {
        // Inspect ONLY this function's block bounded by balanced braces { ... }
        // after signature
        const openBrace = allFunctionSource.indexOf(
          '{',
          match.index + match[0].length,
        );
        const fnBody =
          openBrace !== -1
            ? this.extractBracedBlock_(allFunctionSource, openBrace)
            : '';
        const newClassMatches = fnBody.matchAll(/new\s+([A-Za-z0-9_]+)\s*\(/g);
        for (const classMatch of newClassMatches) {
          const cls = classMatch[1];
          if (classMap.has(cls)) {
            classes.add(cls);
          }
        }
        if (/DriveApp\b/.test(fnBody) && classMap.has('Drive')) {
          classes.add('Drive');
        }
        if (/SpreadsheetApp\b/.test(fnBody) && classMap.has('Sheets')) {
          classes.add('Sheets');
        }

        // Trace called helper functions within the same scope (e.g. gcloud.xxx,
        // sentinel.xxx, or internal xxx_)
        const helperCallMatches = fnBody.matchAll(
          /(?:(?:gcloud|sentinel|tentacles|MojoFunctions)\.([A-Za-z0-9_]+)|([A-Za-z0-9_]+_))\s*\(/g,
        );
        for (const hMatch of helperCallMatches) {
          const calledFn = hMatch[1] || hMatch[2];
          if (calledFn && calledFn !== cleanFnName) {
            const helperClasses = resolveFunctionClasses(calledFn, visited);
            helperClasses.forEach(cls => classes.add(cls));
          }
        }
      }

      // Check if this is an alias assignment: gcloud.fnName = gcloud.targetFn;
      const aliasRegex = new RegExp(
        `(?:(?:gcloud|sentinel|tentacles|MojoFunctions)\\.${cleanFnName}|${
          cleanFnName
        })\\s*=\\s*(?:(?:gcloud|sentinel|tentacles|MojoFunctions)\\.([A-Za-z0-9_]+)|([A-Za-z0-9_]+));`,
      );
      const aliasMatch = allFunctionSource.match(aliasRegex);
      if (aliasMatch) {
        const targetFn = aliasMatch[1] || aliasMatch[2];
        if (targetFn && targetFn !== cleanFnName) {
          const aliasClasses = resolveFunctionClasses(targetFn, visited);
          aliasClasses.forEach(cls => classes.add(cls));
        }
      }
      const result = Array.from(classes);
      functionClassCache.set(cleanFnName, result);
      return result;
    };

    // Resolves a template name to all its checkFn and enableFn across template
    // inheritance chain
    const templateFunctionCache = new Map();
    const resolveTemplateFunctions = (templateName, visited = new Set()) => {
      if (!templateName || visited.has(templateName)) return [];
      visited.add(templateName);

      if (templateFunctionCache.has(templateName)) {
        return templateFunctionCache.get(templateName);
      }

      const fnNames = new Set();
      const templateRegex = new RegExp(
        `(?:['"]?${
          templateName
        }['"]?\\s*:\\s*\\{|MojoTemplateRegistry\\.register\\(['"]${
          templateName
        }['"]\\s*,\\s*\\{|(?:[A-Za-z0-9_$.]+\\.)?${
          templateName
        }\\s*=\\s*\\{|[A-Za-z0-9_$.]+\\[['"]${
          templateName
        }['"]\\]\\s*=\\s*\\{)`,
        'g',
      );
      const match = templateRegex.exec(allTemplateSource);
      if (match) {
        const templateBody = this.extractBracedBlock_(
          allTemplateSource,
          match.index,
        );
        const checkMatch = templateBody.match(/checkFn\s*:\s*([A-Za-z0-9_.]+)/);
        const enableMatch = templateBody.match(
          /enableFn\s*:\s*([A-Za-z0-9_.]+)/,
        );
        if (checkMatch) fnNames.add(checkMatch[1]);
        if (enableMatch) fnNames.add(enableMatch[1]);

        // 1. Explicit template inheritance: template: 'parentTemplate' (support
        // multiple/nested)
        const parentMatches = templateBody.matchAll(
          /template\s*:\s*['"]([A-Za-z0-9_]+)['"]/g,
        );
        for (const pMatch of parentMatches) {
          const parentName = pMatch[1];
          if (parentName !== templateName) {
            const parentFns = resolveTemplateFunctions(parentName, visited);
            parentFns.forEach(fn => fnNames.add(fn));
          }
        }

        // 2. JS Spread inheritance: ...parentTemplate or
        // ...MOJO_RESOURCE_TEMPLATE.parentTemplate
        const spreadMatches = templateBody.matchAll(
          /\.\.\.(?:[A-Za-z0-9_$.]+\.)?([A-Za-z0-9_]+)\b/g,
        );
        for (const sMatch of spreadMatches) {
          const spreadTarget = sMatch[1];
          if (spreadTarget !== templateName) {
            const parentFns = resolveTemplateFunctions(spreadTarget, visited);
            parentFns.forEach(fn => fnNames.add(fn));
          }
        }

        // 3. JS Object.assign inheritance: Object.assign(..., parentTemplate,
        // ...)
        const assignMatches = templateBody.matchAll(
          /Object\.assign\s*\([^,]+,\s*(?:[A-Za-z0-9_$.]+\.)?([A-Za-z0-9_]+)/g,
        );
        for (const aMatch of assignMatches) {
          const assignTarget = aMatch[1];
          if (assignTarget !== templateName) {
            const parentFns = resolveTemplateFunctions(assignTarget, visited);
            parentFns.forEach(fn => fnNames.add(fn));
          }
        }
      }
      const result = Array.from(fnNames);
      templateFunctionCache.set(templateName, result);
      return result;
    };

    // 5. Determine included Cyborg library files so active sheet/UI components
    // can also be scanned for internal API usages
    if (!this.includedCyborgFiles) {
      const cyborgSource = new CyborgSource(cyborgLibSource);
      cyborgSource.load();
      const cyborgConfigFile = path.join(this.solutionSource, 'cyborg.json');
      let explicitIncludes = [];
      if (fs.existsSync(cyborgConfigFile)) {
        const cyborgConfig = fse.readJsonSync(cyborgConfigFile);
        explicitIncludes = cyborgConfig.includes || [];
      }
      const {inferredRoots, originMap} =
        this.inferProjectIncludes_(cyborgSource);
      const combinedRoots = Array.from(
        new Set([...explicitIncludes, ...inferredRoots]),
      );
      const {includes} = cyborgSource.resolveDependencyWithTrace(
        combinedRoots,
        originMap,
      );
      this.includedCyborgFiles = includes || [];
    }

    let scannableCode = solutionCombinedCode;
    this.includedCyborgFiles.forEach(relFile => {
      if (
        relFile.endsWith('.js') &&
        (relFile.includes('5_sheet_element') || relFile.includes('6_sheet'))
      ) {
        const fullPath = path.join(cyborgLibSource, relFile);
        if (fs.existsSync(fullPath)) {
          scannableCode +=
            '\n' + this.stripComments_(fs.readFileSync(fullPath, 'utf8'));
        }
      }
    });

    // 6. Trace all declared templates, check/enable functions, and direct class
    // usages in solutionSource and included sheet/UI components
    const templateMatches = scannableCode.matchAll(
      /template\s*:\s*['"]([A-Za-z0-9_]+)['"]/g,
    );
    for (const tMatch of templateMatches) {
      const templateName = tMatch[1];
      const functions = resolveTemplateFunctions(templateName);
      functions.forEach(fn => {
        const classes = resolveFunctionClasses(fn);
        classes.forEach(cls => addClassMetadata(cls));
      });
    }

    // Extract inline checkFn / enableFn
    const inlineFnMatches = scannableCode.matchAll(
      /(?:checkFn|enableFn)\s*:\s*([A-Za-z0-9_.]+)/g,
    );
    for (const fMatch of inlineFnMatches) {
      const fn = fMatch[1];
      const classes = resolveFunctionClasses(fn);
      classes.forEach(cls => addClassMetadata(cls));
    }

    // Direct ClassName references (new ClassName, ClassName.ATTRIBUTE_NAMES,
    // ClassName.MODE, etc.) in solution source and active sheet/UI components
    const codeWithoutStrings = scannableCode.replace(
      /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)/g,
      '',
    );
    for (const [cls] of classMap) {
      const directClassRegex = new RegExp(
        `(?:\\bnew\\s+${cls}\\b|(?<!\\.)\\b${cls}\\.[A-Za-z0-9_$]+|\\bextends\\s+${cls}\\b)`,
      );
      if (directClassRegex.test(codeWithoutStrings)) {
        addClassMetadata(cls);
      }
    }

    // Foundational GCP permissions
    if (detectedApis.size > 0) {
      detectedPermissions.add('resourcemanager.projects.get');
      permissionToRoleMap.set('resourcemanager.projects.get', 'Viewer');

      detectedPermissions.add('serviceusage.services.enable');
      permissionToRoleMap.set(
        'serviceusage.services.enable',
        'Service Usage Admin',
      );
    }

    // Determine output target directory: source apps_script folder
    const sourceAppsScriptDir = fs.existsSync(
      path.join(this.solutionSource, APPS_SCRIPT_FOLDER),
    )
      ? path.join(this.solutionSource, APPS_SCRIPT_FOLDER)
      : this.solutionSource;

    let autogenContent = '';

    // 7. Generate DEFAULT_API
    if (detectedApis.size > 0) {
      const sortedApiEntries = Array.from(detectedApis.entries())
        .map(([api, name]) => ({name, api}))
        .sort((a, b) => a.name.localeCompare(b.name));

      const names = sortedApiEntries.map(e => e.name);
      const apis = sortedApiEntries.map(e => e.api);

      const defaultApiObj = {
        name: names,
        api: apis,
      };

      autogenContent += `const DEFAULT_API = ${JSON.stringify(defaultApiObj, null, 2)};\n\n`;
    }

    // 8. Generate DEFAULT_PERMISSIONS
    if (detectedPermissions.size > 0 || detectedRuntimeRoles.size > 0) {
      const sortedPermissions = Array.from(detectedPermissions).sort();
      const rolesForPermissions = sortedPermissions.map(
        p => permissionToRoleMap.get(p) || 'Editor',
      );

      const defaultPermObj = {
        permissions: sortedPermissions,
        rolesForPermissions: rolesForPermissions,
        roles: Array.from(detectedRuntimeRoles).sort(),
      };

      autogenContent += `const DEFAULT_PERMISSIONS = ${JSON.stringify(
        defaultPermObj,
        null,
        2,
      )};\n`;
    }

    if (autogenContent.trim().length > 0) {
      const autogenFilePath = path.join(
        this.destAppsScriptRoot,
        '_autogen_config.js',
      );
      fse.outputFileSync(autogenFilePath, autogenContent);
      console.log(
        `> Generated _autogen_config.js (${detectedApis.size} APIs, ${
          detectedPermissions.size
        } permissions, ${detectedRuntimeRoles.size} runtime roles) in ${
          this.destAppsScriptRoot
        }`,
      );
    }

    // 9. Collect dynamic getScope() mappings from API class registry
    const fileScopeMap = new Map();
    for (const [cls, desc] of classMap.entries()) {
      if (desc.scope && desc.fileName) {
        fileScopeMap.set(desc.fileName, desc.scope);
      }
    }

    // 10. Generate dynamic appsscript.json directly in destAppsScriptRoot
    // (reading source as overlay if present)
    this.generateAppsscriptManifest_(
      detectedApis,
      solutionCombinedCode,
      sourceAppsScriptDir,
      this.destAppsScriptRoot,
      this.includedCyborgFiles,
      fileScopeMap,
    );
  }

  /**
   * Dynamically derives and generates `appsscript.json` in the destination
   * build folder.
   * @param {!Map<string, string>} detectedApis
   * @param {string} solutionCombinedCode
   * @param {string} sourceAppsScriptDir Read-only source directory for user
   *     overlay.
   * @param {string} destAppsScriptDir Target build directory where
   *     appsscript.json will be written.
   * @param {!Array<string>=} includedCyborgFiles Array of included Cyborg
   *     library files.
   * @param {!Map<string, string>=} fileScopeMap Dynamic mapping of fileName ->
   *     getScope().
   * @private
   */
  generateAppsscriptManifest_(
    detectedApis,
    solutionCombinedCode,
    sourceAppsScriptDir,
    destAppsScriptDir,
    includedCyborgFiles = [],
    fileScopeMap = new Map(),
  ) {
    // 1. Infer TimeZone
    let timeZone = 'Australia/Sydney';
    const tzMatch = solutionCombinedCode.match(
      /template\s*:\s*['"]timeZone['"]\s*,\s*value\s*:\s*['"]([^'"]+)['"]/,
    );
    if (tzMatch) {
      timeZone = tzMatch[1];
    } else if (this.config && this.config.timeZone) {
      timeZone = this.config.timeZone;
    }

    // 2. Derive OAuth Scopes
    const scopes = new Set([
      'openid',
      'https://www.googleapis.com/auth/userinfo.profile',
      'https://www.googleapis.com/auth/script.external_request',
      'https://www.googleapis.com/auth/script.scriptapp',
      'https://www.googleapis.com/auth/script.container.ui',
      'https://www.googleapis.com/auth/spreadsheets',
    ]);

    // Add GCP platform scope if any GCP service was detected
    if (detectedApis.size > 0) {
      scopes.add('https://www.googleapis.com/auth/cloud-platform');
    }

    if (detectedApis.has('script.googleapis.com')) {
      scopes.add('https://www.googleapis.com/auth/script.deployments');
      scopes.add('https://www.googleapis.com/auth/script.projects');
    }

    // Direct Apps Script Workspace service references in solution source
    if (/\bDriveApp\b/.test(solutionCombinedCode)) {
      scopes.add('https://www.googleapis.com/auth/drive');
    }
    if (/\bGmailApp\b/.test(solutionCombinedCode)) {
      scopes.add('https://www.googleapis.com/auth/gmail.send');
    }

    // Automatically inject scopes declared by included API classes via
    // getScope()
    const includedBaseNames = new Set(
      includedCyborgFiles.map(f => path.basename(f)),
    );

    includedBaseNames.forEach(fileName => {
      if (fileScopeMap.has(fileName)) {
        scopes.add(fileScopeMap.get(fileName));
      }
    });

    const manifestObj = {
      timeZone,
      dependencies: {
        enabledAdvancedServices: [],
      },
      exceptionLogging: 'STACKDRIVER',
      runtimeVersion: 'V8',
      executionApi: {
        access: 'MYSELF',
      },
      oauthScopes: Array.from(scopes),
    };

    // 3. Detect Web App (doGet / doPost)
    const isWebApp =
      /\b(?:function\s+(?:doGet|doPost)|(?:doGet|doPost)\s*=\s*(?:function|\([^)]*\)\s*=>))\b/.test(
        solutionCombinedCode,
      );
    if (isWebApp) {
      manifestObj.webapp = {
        executeAs: 'USER_DEPLOYING',
        access: 'DOMAIN',
      };
    }

    // 4. Merge with existing user appsscript.json if present in source
    // (read-only overlay)
    const sourceManifestPath = path.join(
      sourceAppsScriptDir,
      'appsscript.json',
    );
    if (fs.existsSync(sourceManifestPath)) {
      try {
        const userManifest = fse.readJsonSync(sourceManifestPath);
        const mergedScopes = Array.from(
          new Set([
            ...manifestObj.oauthScopes,
            ...(userManifest.oauthScopes || []),
          ]),
        );
        Object.assign(manifestObj, userManifest, {
          oauthScopes: mergedScopes,
          webapp: userManifest.webapp || manifestObj.webapp,
        });
      } catch (err) {
        console.warn(
          `> Warning: Failed to parse user-defined ${sourceManifestPath}:`,
          err.message,
        );
      }
    }

    // 5. Output directly to build destination directory (never write back to
    // src)
    const destManifestPath = path.join(destAppsScriptDir, 'appsscript.json');
    fse.outputJsonSync(destManifestPath, manifestObj, {spaces: 2});
    console.log(
      `> Generated appsscript.json (${manifestObj.oauthScopes.length} scopes${
        manifestObj.webapp ? ', webapp enabled' : ''
      }) in ${destAppsScriptDir}`,
    );
  }

  /**
   * Parses .gcloudignore (or .gitignore) lines into filter rules.
   * @param {string} ignoreFilePath Path to ignore file.
   * @return {!Array<string>} Filter rules.
   * @private
   */
  parseIgnoreRules_(ignoreFilePath) {
    if (!fs.existsSync(ignoreFilePath)) return [];
    return fs
      .readFileSync(ignoreFilePath, 'utf8')
      .split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#'));
  }

  /**
   * Checks if a relative path matches any ignore rule from .gcloudignore /
   * .gitignore.
   * @param {string} relPath POSIX-style relative path.
   * @param {boolean} isDir Whether the path is a directory.
   * @param {!Array<string>} rules Array of ignore rules.
   * @return {boolean} True if path should be ignored.
   * @private
   */
  shouldIgnorePath_(relPath, isDir, rules) {
    const normalized = relPath.split(path.sep).join('/');
    const segments = normalized.split('/');

    for (const rawRule of rules) {
      let rule = rawRule.split(path.sep).join('/');
      if (rule.endsWith('/')) rule = rule.slice(0, -1);

      // Match exact segment name (e.g. 'node_modules', '.git')
      if (segments.includes(rule)) return true;

      // Wildcard glob matching (e.g. 'blaze-*', '*.test.js', '.env.*')
      if (rule.includes('*')) {
        const regexPattern =
          '^' + rule.replace(/\./g, '\\.').replace(/\*/g, '.*') + '$';
        const regex = new RegExp(regexPattern);
        if (segments.some(seg => regex.test(seg))) return true;
        if (regex.test(normalized)) return true;
      }

      // Direct path match or directory prefix match
      if (normalized === rule || normalized.startsWith(rule + '/')) return true;
    }
    return false;
  }

  /**
   * Recursively collects relative file paths in a directory excluding ignored
   * paths.
   * @param {string} rootDir Root directory to scan.
   * @param {string} subDir Current subdirectory path relative to rootDir.
   * @param {!Array<string>} rules Ignore rules.
   * @return {!Array<string>} List of included relative file paths.
   * @private
   */
  collectFilesToZip_(rootDir, subDir = '', rules = []) {
    let results = [];
    const currentDir = subDir ? path.join(rootDir, subDir) : rootDir;
    if (!fs.existsSync(currentDir)) return results;

    const entries = fs.readdirSync(currentDir, {withFileTypes: true});
    for (const entry of entries) {
      const relPath = subDir ? path.join(subDir, entry.name) : entry.name;
      const isDir = entry.isDirectory();
      if (this.shouldIgnorePath_(relPath, isDir, rules)) continue;

      if (isDir) {
        results = results.concat(
          this.collectFilesToZip_(rootDir, relPath, rules),
        );
      } else {
        results.push(relPath);
      }
    }
    return results;
  }

  /**
   * Updates embedded source assets into `_embedded_source.js` based on
   * `cyborg.json` embedded configuration.
   */
  updateEmbedded() {
    const cyborgConfigFile = path.join(this.solutionSource, 'cyborg.json');
    if (!fs.existsSync(cyborgConfigFile)) {
      return;
    }

    const cyborgConfig = fse.readJsonSync(cyborgConfigFile);
    if (
      !cyborgConfig.embedded ||
      !Array.isArray(cyborgConfig.embedded) ||
      cyborgConfig.embedded.length === 0
    ) {
      return;
    }

    console.log(
      `> Processing ${
        cyborgConfig.embedded.length
      } embedded items from cyborg.json...`,
    );
    const embeddedResult = {};

    cyborgConfig.embedded.forEach(item => {
      if (!item.variableName || !item.sourcePath) {
        console.warn(
          '> Warning: embedded item missing variableName or sourcePath:',
          item,
        );
        return;
      }

      const resolvedPath = path.isAbsolute(item.sourcePath)
        ? item.sourcePath
        : path.resolve(this.solutionSource, item.sourcePath);

      if (!fs.existsSync(resolvedPath)) {
        console.error(
          `> Error: embedded sourcePath does not exist: ${resolvedPath}`,
        );
        return;
      }

      const stat = fs.statSync(resolvedPath);
      if (stat.isFile()) {
        // Source is a file -> Read text content directly
        const content = fs.readFileSync(resolvedPath, 'utf8');
        embeddedResult[item.variableName] = content;
        console.log(
          `  • [${item.variableName}] Loaded file: ${resolvedPath} (${
            content.length
          } chars)`,
        );
      } else if (stat.isDirectory()) {
        // Source is a directory -> Follow .gcloudignore / .gitignore and zip
        const ignoreRules = this.parseIgnoreRules_(
          path.join(resolvedPath, '.gcloudignore'),
        );
        if (ignoreRules.length === 0) {
          ignoreRules.push(
            ...this.parseIgnoreRules_(path.join(resolvedPath, '.gitignore')),
          );
        }
        // Default baseline exclusions
        ['.git', '.gcloudignore', '.gitignore'].forEach(rule => {
          if (!ignoreRules.includes(rule)) ignoreRules.push(rule);
        });

        const filesToZip = this.collectFilesToZip_(
          resolvedPath,
          '',
          ignoreRules,
        );
        console.log(
          `  • [${item.variableName}] Zipping directory: ${resolvedPath} (${
            filesToZip.length
          } files matched)`,
        );

        // Ensure temp directory exists in destination
        fse.ensureDirSync(this.destination);
        const tempZipFile = path.join(
          this.destination,
          `_temp_${item.variableName}_${Date.now()}.zip`,
        );

        try {
          execFileSync('zip', ['-q', tempZipFile, '-@'], {
            cwd: resolvedPath,
            input: filesToZip.join('\n') + '\n',
          });

          const zipBuffer = fs.readFileSync(tempZipFile);
          const base64Content = zipBuffer.toString('base64');
          embeddedResult[item.variableName] = base64Content;
          console.log(
            `    - Zipped size: ${zipBuffer.length} bytes, base64 length: ${
              base64Content.length
            } chars`,
          );
        } finally {
          fse.removeSync(tempZipFile);
        }
      }
    });

    if (Object.keys(embeddedResult).length > 0) {
      const sourceAppsScriptDir = fs.existsSync(
        path.join(this.solutionSource, APPS_SCRIPT_FOLDER),
      )
        ? path.join(this.solutionSource, APPS_SCRIPT_FOLDER)
        : this.solutionSource;

      const fileContent =
        'const EMBEDDED_SOURCE = ' +
        JSON.stringify(embeddedResult, null, 2) +
        ';\n';

      const targetPath = path.join(sourceAppsScriptDir, '_embedded_source.js');
      fse.outputFileSync(targetPath, fileContent);
      console.log(
        `> Generated _embedded_source.js (${
          Object.keys(embeddedResult).length
        } variables) in ${sourceAppsScriptDir}`,
      );
    }
  }

  /** Prepares the source files to be pushed or minified. */
  prepareCodeToBePushed(cyborgLibSource) {
    this.init();
    this.copyCyborgLibrary(cyborgLibSource); // Copy Cyborg lib files
    this.copyProjectFiles(); // Copy solution files to dest
    this.generateDefaultConfigs(cyborgLibSource); // Generate _autogen_config.js & appsscript.json
    // directly in dest
  }

  /** Minifies Cyborg source codes. */
  compress() {
    // Cyborg JavaScript files will be merged and minified to '_cyborg.min.js'.
    this.compressFilesToSingle(
      '_cyborg.min.js',
      `${this.destAppsScriptRoot}/*/**/*.js`,
    );
    // Cyborg other files will be copied.
    this.copyFilesToCompress(`${this.destAppsScriptRoot}/*/**`, {
      nodir: true,
      ignore: '**/*.js',
    });
    // Project files are under root path.
    this.copyFilesToCompress(`${this.destAppsScriptRoot}/*`, {nodir: true});
  }

  /** Minifies partial Cyborg source codes based on given folders. */
  partiallyCompress(uncompress = this.uncompressArg) {
    if (!uncompress) {
      console.log(
        `> Compress: No 'uncompress' was set, will compress all Cyborg files`,
      );
      this.compress();
      return;
    }
    const exposeFolders = uncompress.split(',').map(folder => folder.trim());
    console.log(
      '> Compress: Will not compress folders end with: ',
      exposeFolders,
    );
    const folders = globSync(`${this.destAppsScriptRoot}/*/`).sort();
    console.log(' + Cyborg folders', folders);
    folders.forEach(folder => {
      const folderName = folder.substring(folder.lastIndexOf('/') + 1);
      if (!exposeFolders.some(name => folderName.endsWith(name))) {
        // Cyborg JavaScript files will be minified.
        this.compressFilesToSingle(`${folderName}.min.js`, `${folder}/**/*.js`);
        // Cyborg other files will be copied.
        this.copyFilesToCompress(`${folder}/**`, {
          nodir: true,
          ignore: '**/*.js',
        });
      } else {
        // Copy source files for uncompressed folder.
        this.copyFilesToCompress(`${folder}/**`, {nodir: true});
      }
    });
    // Project files are under root path.
    this.copyFilesToCompress(`${this.destAppsScriptRoot}/*`, {nodir: true});
  }

  /** Copies files to the compress folder. */
  copyFilesToCompress(pattern, options) {
    const files = globSync(pattern, options);
    if (files.length === 0) return;
    console.log(` - Copy ${files.length} files to compress folder:`, files);
    files.forEach(file => {
      const relativePath = file.replace(this.destAppsScriptRoot, '');
      fse.copySync(file, path.join(this.compressedDesc, relativePath));
    });
  }

  /** Minifies files to a single one in the compress folder. */
  compressFilesToSingle(fileName, pattern, options = {nodir: true}) {
    const files = globSync(pattern, options).sort();
    if (files.length === 0) return;
    console.log(` - Merge ${files.length} files to ${fileName}`, files);
    const contents = files.map(file => fse.readFileSync(file).toString());
    const minified = UglifyJS.minify(contents.join('\n'));
    fse.outputFileSync(path.join(this.compressedDesc, fileName), minified.code);
  }

  /** Generates '.clasp.json' in destination folder `clasp` to push codes. */
  generateClaspJson(rootDir) {
    console.log('> Generate .clasp.json:', rootDir);
    fs.writeFileSync(
      path.join(rootDir, '.clasp.json'),
      JSON.stringify({
        scriptId: this.config.env[this.target].scriptId,
        rootDir,
      }),
    );
  }

  /** Uses `clasp` to push the codes. */
  clashPush(compressed = true, onSuccess = null) {
    const rootDir = compressed ? this.compressedDesc : this.destAppsScriptRoot;
    this.generateClaspJson(rootDir); // '.clasp.json' file for clasp
    console.log(`> Start pushing code from [${rootDir}]...`);
    execFile('clasp', ['push', '-f', '-P', rootDir], (error, data, stderr) => {
      if (error) {
        console.error(
          `[ERROR]: Failed to push code to '${this.solution}@${this.target}'`,
        );
        console.error(error);
        return;
      }
      const errorMessage = stderr.toString();
      if (errorMessage.toLowerCase().indexOf('error') > -1) {
        console.error(stderr.toString());
      } else {
        console.log(data.toString());
        if (onSuccess) onSuccess(rootDir);
      }
    });
  }

  /** Opens the Apps Script editor page. */
  openScript() {
    console.log(
      `Start opening Apps Script of '${this.solution}@${this.target}'...`,
    );
    const scriptId = this.config.env[this.target].scriptId;
    if (!scriptId) {
      console.error(
        `[ERROR]: Failed to open Apps Script '${this.solution}@${this.target}'`,
      );
      process.exit(-1);
    }
    open(`https://script.google.com/d/${scriptId}/edit`);
  }

  /** Opens the Google Sheets. */
  openSheet() {
    console.log(
      `Start opening Google Sheets of '${this.solution}@${this.target}'...`,
    );
    const sheetId = this.config.env[this.target].parentId;
    if (!sheetId) {
      console.error(
        `[ERROR]: Failed to find Sheet ID for '${this.solution}@${
          this.target
        }'`,
      );
      process.exit(-1);
    }
    open(`https://drive.google.com/open?id=${sheetId}`);
  }

  /** Uses `clasp` to run the function. */
  runFunction(fn) {
    const rootDir = this.destAppsScriptRoot;
    this.generateClaspJson(rootDir); // '.clasp.json' file for clasp
    console.log(`> Starting clasp running function from [${rootDir}]...`);
    execFile('clasp', ['run', fn, '-P', rootDir], (error, data, stderr) => {
      if (error) {
        console.error(
          `[ERROR]: Failed to run [${fn}]: '${this.solution}@${this.target}'`,
        );
        console.error(error);
      }
      const errorMessage = stderr.toString();
      if (errorMessage.toLowerCase().indexOf('error') > -1) {
        console.error(stderr.toString());
      } else {
        console.log(data.toString());
      }
    });
  }

  /**
   * Deploys the Apps Script project.
   * Fetches existing deployments and updates the last one to keep the URL
   * unchanged.
   */
  deploy(rootDir) {
    console.log(`> Start deploying from [${rootDir}]...`);
    const {deploymentId} = this.config.env[this.target];
    if (deploymentId) {
      console.log(` - Found the given deployment ID: [${deploymentId}]`);
      this.deployWithDeploymentId_(rootDir, deploymentId);
    } else {
      console.log(`> Fetching deployments from [${rootDir}]...`);
      execFile(
        'clasp',
        ['deployments', '-P', rootDir],
        (error, data, stderr) => {
          if (error) {
            console.error(
              `[ERROR]: Failed to fetch deployments: '${this.solution}@${
                this.target
              }'`,
            );
            console.error(error);
            return;
          }
          const output = data.toString();
          const lines = output
            .split('\n')
            .filter(line => line.startsWith('- '));
          let deploymentId;
          if (lines.length === 0) {
            console.log(
              ' - No existing deployments found. Creating a new one...',
            );
          } else {
            // Get the last deployment from the list
            const lastLine = lines[lines.length - 1];
            // Extract the deployment ID using regex matching "- {id} @"
            const match = lastLine.match(/- ([a-zA-Z0-9-_]+) @/);
            if (match && match[1]) {
              deploymentId = match[1];
              console.log(` - Using latest deployment ID: [${deploymentId}]`);
            } else {
              console.error(
                `[ERROR]: Failed to parse deployment ID from string: ${
                  lastLine
                }`,
              );
              return;
            }
          }
          this.deployWithDeploymentId_(rootDir, deploymentId);
        },
      );
    }
  }

  /**
   * Deploy Apps Script with the given deployment Id.
   * @private
   */
  deployWithDeploymentId_(rootDir, deploymentId) {
    const {deploymentDesc = 'Auto-deployed via Cyborg'} =
      this.config.env[this.target];
    const args = ['deploy', '-d', deploymentDesc, '-P', rootDir];
    if (deploymentId) {
      args.push('-i', deploymentId);
    }
    execFile('clasp', args, (error, data, stderr) => {
      if (error) {
        console.error(
          `[ERROR]: Failed to deploy: '${this.solution}@${this.target}'`,
        );
        console.error(error);
      }
      const errorMessage = stderr.toString();
      if (errorMessage.toLowerCase().indexOf('error') > -1) {
        console.error(errorMessage);
      } else {
        console.log(data.toString());
      }
    });
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
};

/**
 * Helper to extract arguments from process.argv.
 * Supports both --key=value and --key value formats.
 * @param {string} name The argument name (e.g., 'solution').
 * @return {string|null}
 */
const getArgValue = name => {
  const args = process.argv;
  const index = args.findIndex(arg => arg.startsWith(`--${name}=`));
  if (index !== -1) {
    return args[index].split('=')[1];
  }
  // Search for --name value
  const namedIndex = args.indexOf(`--${name}`);
  if (namedIndex !== -1 && args[namedIndex + 1]) {
    return args[namedIndex + 1];
  }
  return null;
};

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
  // Solution to be pushed
  let solution = getArgValue('solution');
  // Env (dev, test, prod, etc.) to be pushed
  const target = getArgValue('target') || 'dev';
  // Use getArgValue for other possible flags like 'source' or 'uncompress'
  const sourceArg = getArgValue('source');
  const uncompressArg = getArgValue('uncompress');
  const solutions = getSolutions(solutionsFile, allowEmpty);
  if (!solution && Object.keys(solutions).length === 1) {
    solution = Object.keys(solutions)[0];
  }
  // Configurations of the solution to be pushed
  const config = solutions[solution] || (allowEmpty ? {} : undefined);
  if (!config) {
    console.error(`[ERROR]: Solution[${solution}] doesn't exist.`);
    process.exit(-1);
  }
  if (!config.env || !config.env[target]) {
    if (!allowEmpty) {
      console.error(
        `[ERROR]: Solution[${solution}] doesn't have target [${target}]`,
      );
      process.exit(-1);
    }
  }
  return {solution, target, config, sourceArg, uncompressArg};
};

/**
 * Creates a new Google Sheets for the specified solution and target and
 * adds the new file into `SOLUTIONS_CONFIG_FILE`.
 * @param {string} baseDir
 * @param {string} solutionsFile
 */
const create = (baseDir, solutionsFile) => {
  const solutions = getSolutions(solutionsFile, true);
  const {solution, target, config, sourceArg} = getConfig(solutionsFile, true);
  if (config.env && config.env[target]) {
    console.error(
      `[ERROR]: Solution '${solution}@${target}' exists:`,
      config.env[target],
    );
    process.exit(-1);
  }
  if (config.source && sourceArg && config.source !== sourceArg) {
    console.log(
      `[WARN]: Solution[${solution}] has source '${config.source}'. Ignore:`,
      sourceArg,
    );
  }
  const source = config.source || sourceArg || `./src/${solution}`;
  if (!fs.existsSync(source)) {
    fs.mkdirSync(path.join(source, APPS_SCRIPT_FOLDER), {recursive: true});
  }
  const env = config.env || {};
  console.log(
    `Start creating a new Google Sheets for ${solution}@[${target}]...`,
  );
  execFile(
    'clasp',
    ['create', '--type', 'sheets', '--title', solution],
    (error, data, stderr) => {
      if (error) {
        console.error(
          `[ERROR]: Failed to open Apps Script '${solution}@${target}'`,
        );
        console.error(error);
        process.exit(-1);
      }
      const errorMessage = stderr.toString();
      if (errorMessage.toLowerCase().indexOf('error') > -1) {
        console.error(stderr.toString());
      } else {
        console.log(data.toString());
        const claspFile = path.join(baseDir, '.clasp.json');
        const {scriptId, parentId} = JSON.parse(
          fs.readFileSync(claspFile, 'utf8'),
        );
        env[target] = {scriptId, parentId};
        solutions[solution] = {source, env};
        fse.outputFileSync(solutionsFile, JSON.stringify(solutions));
        fse.removeSync(claspFile);
        fse.removeSync(path.join(baseDir, 'appsscript.json'));
        console.log(`> Updated config for ${solution}@[${target}]...`);
      }
    },
  );
};

/**
 * Lists solutions configurations.
 * @param {string} solutionsFile
 */
const list = solutionsFile => {
  console.log('Start listing solutions from:', solutionsFile, '\n');
  const solutions = getSolutions(solutionsFile);
  Object.keys(solutions).forEach(solution => {
    const config = solutions[solution];
    console.log(`Solution [${solution}], source:`, config.source);
    Object.keys(config.env).forEach(env => {
      const {parentId, scriptId} = config.env[env];
      console.log('  └─ [%s] Google Sheets Id: %s', env, parentId);
      console.log('       └─  ScriptId: %s', scriptId);
    });
    console.log();
  });
};

const argv = process.argv;
const cmd = argv[2];

// Solution root path
const baseDir = path.resolve();
// Solution configurations file
const solutionsFile = path.join(baseDir, SOLUTIONS_CONFIG_FILE);
// Cyborg library source code path
const cyborgLibSource = path.join(
  path.dirname(fs.realpathSync(process.argv[1])),
  '../',
  'src',
);
const getCyborgSolution = () => {
  const {solution, target, config, uncompressArg} = getConfig(solutionsFile);
  console.log(`Start ${cmd}ing [${solution}]@${target} ...`);
  return new CyborgSolution(baseDir, config, solution, target, uncompressArg);
};

if (cmd === 'create') {
  create(baseDir, solutionsFile);
} else if (cmd === 'list') {
  list(solutionsFile);
} else if (cmd === 'open' || cmd === 'openScript') {
  getCyborgSolution().openScript();
} else if (cmd === 'openSheet') {
  getCyborgSolution().openSheet();
} else if (cmd === 'push') {
  const cyborg = getCyborgSolution();
  cyborg.prepareCodeToBePushed(cyborgLibSource);
  cyborg.partiallyCompress();
  cyborg.clashPush();
} else if (cmd === 'debug') {
  const cyborg = getCyborgSolution();
  cyborg.prepareCodeToBePushed(cyborgLibSource);
  cyborg.clashPush(false);
} else if (cmd === 'init') {
  const cyborg = getCyborgSolution();
  cyborg.prepareCodeToBePushed(cyborgLibSource);
  cyborg.runFunction('initializeApplication');
} else if (cmd === 'trace') {
  const cyborg = getCyborgSolution();
  cyborg.prepareCodeToBePushed(cyborgLibSource);
} else if (cmd === 'embed') {
  const cyborg = getCyborgSolution();
  cyborg.updateEmbedded();
} else if (cmd === 'deploy') {
  const cyborg = getCyborgSolution();
  cyborg.prepareCodeToBePushed(cyborgLibSource);
  cyborg.partiallyCompress();
  cyborg.clashPush(true, rootDir => {
    cyborg.deploy(rootDir);
  });
} else {
  console.log(`cyborg: unrecognized command ${cmd}`);
  console.log(
    'usage: npm exec cyborg create -- --solution=SOLUTION [--source=./src/SOLUTION] [--target=dev]',
  );
  console.log('       npm exec cyborg list');
  console.log(
    '       npm exec cyborg trace -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg embed -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg push -- --solution=SOLUTION [--target=ENV] [--uncompress=FOLDERS]',
  );
  console.log(
    '       npm exec cyborg debug -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg init -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg deploy -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg open|openScript -- --solution=SOLUTION [--target=ENV]',
  );
  console.log(
    '       npm exec cyborg openSheet -- --solution=SOLUTION [--target=ENV]',
  );
}
