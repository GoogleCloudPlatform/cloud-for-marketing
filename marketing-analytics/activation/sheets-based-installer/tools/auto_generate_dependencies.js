/**
 * Automated Apps Script Dependency Graph Generator
 * Scans src/apps_script/ source files to extract declared top-level symbols
 * (classes, functions, constants) and maps references across files to build
 * src/dependency.json automatically without manual bookkeeping.
 */

import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');
const APPS_SCRIPT_DIR = path.join(ROOT_DIR, 'src', 'apps_script');
const TARGET_JSON_PATH = path.join(ROOT_DIR, 'src', 'dependency.json');

// Built-in JavaScript and Apps Script environment globals that should never be
// treated as file dependencies
const IGNORED_GLOBALS = new Set([
  'Object',
  'Array',
  'String',
  'Number',
  'Boolean',
  'Date',
  'Math',
  'JSON',
  'RegExp',
  'Error',
  'TypeError',
  'ReferenceError',
  'Promise',
  'Buffer',
  'console',
  'undefined',
  'NaN',
  'globalThis',
  'global',
  'window',
  'document',
  'parseInt',
  'parseFloat',
  'isNaN',
  'encodeURIComponent',
  'Logger',
  'UrlFetchApp',
  'Utilities',
  'ScriptApp',
  'SpreadsheetApp',
  'DriveApp',
  'GmailApp',
  'HtmlService',
  'CacheService',
  'LockService',
  'PropertiesService',
  'Session',
  'CardService',
]);

/**
 * Recursively retrieves all .js and .html files within a directory relative to
 * APPS_SCRIPT_DIR.
 * @param {string} dir Absolute directory path.
 * @return {!Array<string>} Array of absolute file paths.
 */
function scanSourceFiles(dir) {
  const entries = fs.readdirSync(dir, {withFileTypes: true});
  const results = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...scanSourceFiles(fullPath));
    } else if (entry.name.endsWith('.js') || entry.name.endsWith('.html')) {
      results.push(fullPath);
    }
  }
  return results;
}

/**
 * Strips comments from source code while preserving string literals.
 * @param {string} code JavaScript source code.
 * @return {string} Code without comments.
 */
function stripComments(code) {
  return code.replace(
      /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)|\/\*[\s\S]*?\*\/|\/\/.*/g,
      (match, str) => (str ? str : ''),
  );
}

/**
 * Extracts declared top-level symbols (classes, functions, const/let/var
 * identifiers) from code.
 * @param {string} sourceCode JavaScript source contents.
 * @return {!Set<string>} Set of exported symbol names.
 */
function extractDeclaredSymbols(sourceCode) {
  const symbols = new Set();

  // Match class definitions: class ClassName
  const classMatches = sourceCode.matchAll(/^class\s+([A-Za-z0-9_]+)/gm);
  for (const match of classMatches) {
    symbols.add(match[1]);
  }

  // Match top-level function declarations: function foo(
  const funcMatches = sourceCode.matchAll(/^function\s+([A-Za-z0-9_]+)\s*\(/gm);
  for (const match of funcMatches) {
    symbols.add(match[1]);
  }

  // Match top-level const/let/var declarations: const Foo = ...
  const varMatches = sourceCode.matchAll(
      /^(?:const|let|var)\s+([A-Za-z0-9_]+)\s*=/gm,
  );
  for (const match of varMatches) {
    symbols.add(match[1]);
  }

  // Match MojoFunctions namespace function definitions: MojoFunctions.foo = ...
  const mojoFuncMatches = sourceCode.matchAll(
      /^MojoFunctions\.([A-Za-z0-9_]+)\s*=/gm,
  );
  for (const match of mojoFuncMatches) {
    symbols.add('MojoFunctions.' + match[1]);
  }

  // Match MojoTemplateRegistry template declarations:
  // MojoTemplateRegistry.register('foo'
  const templateRegMatches = sourceCode.matchAll(
      /MojoTemplateRegistry\.register\(\s*['"]([A-Za-z0-9_]+)['"]/g,
  );
  for (const match of templateRegMatches) {
    symbols.add(
        'MOJO_TPL:' +
        match[1]);  // Add domain prefix to isolate from JS variables
  }

  return symbols;
}

/**
 * Extracts referenced symbol identifiers and explicit HTML/JS file string
 * references from code.
 * @param {string} sourceCode JavaScript source contents.
 * @return {!Set<string>} Set of referenced symbol identifiers or filename
 *     strings.
 */
function extractReferencedSymbols(sourceCode, isHtml) {
  const refs = new Set();

  // Extract Mojo template references (e.g. template: 'pubsubTopic') from the
  // raw strings before stripping.
  const templateUseMatches = sourceCode.matchAll(
      /template:\s*['"]([A-Za-z0-9_]+)['"]/g,
  );
  for (const match of templateUseMatches) {
    refs.add('MOJO_TPL:' + match[1]);  // Match the domain prefix
  }

  // Extract MojoFunctions namespace method calls (e.g.
  // MojoFunctions.getCloudFunctionDeployStatus)
  const mojoCallMatches = sourceCode.matchAll(
      /\bMojoFunctions\.([A-Za-z0-9_]+)\b/g,
  );
  for (const match of mojoCallMatches) {
    refs.add('MojoFunctions.' + match[1]);
  }

  let codeToScan = sourceCode;

  if (isHtml) {
    // For HTML files, ONLY scan inside <script>...</script> for variable
    // references. Avoids mistaking plain HTML text for JS dependencies.
    const scriptMatches = sourceCode.matchAll(
        /<script(?:\s[^>]*?)?>([\s\S]*?)<\/script>/gi,
    );
    const scripts = [];
    for (const match of scriptMatches) {
      scripts.push(match[1]);
    }
    codeToScan = scripts.join('\n');
  }

  // Strip string literals before extracting code identifiers so string content
  // (e.g. 'BigQuery Admin', 'Cloud Datastore Owner') does not create false code
  // dependencies.
  const codeWithoutStrings = codeToScan.replace(
      /("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|`([^`\\]|\\.)*`)/g,
      '',
  );

  // Extract identifier references (alphanumeric words).
  // Use negative lookbehind (?<!\.) to ignore object property access (e.g.,
  // obj.AppsScript) Use negative lookahead (?!\s*:) to ignore object literal
  // keys (e.g., Gmail: { )
  const idMatches = codeWithoutStrings.matchAll(
      /(?<!\.)\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*:)/g,
  );
  for (const match of idMatches) {
    const sym = match[1];
    if (!IGNORED_GLOBALS.has(sym)) {
      refs.add(sym);
    }
  }

  // Extract explicit references to HTML template resources or JS filenames
  // inside string literals Strings are only in JS files usually, but we can
  // safely run this on the original sourceCode.
  const stringMatches = sourceCode.matchAll(
      /['"]([A-Za-z0-9_/-]+\.(?:html|js))['"]/g,
  );
  for (const match of stringMatches) {
    refs.add(match[1]);
  }

  // Extract explicit HtmlService template/file loading calls:
  // HtmlService.createTemplateFromFile('path/to/template')
  const templateCallMatches = sourceCode.matchAll(
      /HtmlService\.(?:createTemplateFromFile|createHtmlOutputFromFile)\s*\(\s*['"]([^'"]+)['"]/g,
  );
  for (const match of templateCallMatches) {
    refs.add(match[1]);
  }

  return refs;
}

/**
 * Executes full dependency resolution across all discovered Apps Script
 * sources.
 * @return {!Object<string, !Array<string>>} Generated dependency mapping
 *     object.
 */
export function generateDependencyGraph() {
  const filePaths = scanSourceFiles(APPS_SCRIPT_DIR);
  const symbolToFile = new Map();
  const fileRefs = new Map();
  const fileDeclared = new Map();

  // Pass 1: Build symbol table mapping defined symbols and direct basenames to
  // owning file
  for (const fullPath of filePaths) {
    const baseName = path.basename(fullPath);
    const relativePath =
        path.relative(APPS_SCRIPT_DIR, fullPath).replace(/\\/g, '/');
    const rawCode = fs.readFileSync(fullPath, 'utf8');
    const cleanCode = stripComments(rawCode);
    const isHtml = baseName.endsWith('.html');

    // Only JS files export symbols to the global Apps Script server scope
    if (!isHtml) {
      const declared = extractDeclaredSymbols(cleanCode);
      fileDeclared.set(baseName, declared);
      for (const sym of declared) {
        symbolToFile.set(sym, baseName);
      }
    }

    // A file can always be referenced directly by its own filename
    symbolToFile.set(baseName, baseName);
    symbolToFile.set(relativePath, baseName);

    // For HTML files, register relative template paths (e.g. 'pages/oauth') so
    // createTemplateFromFile works
    if (isHtml) {
      symbolToFile.set(relativePath.replace(/\.html$/, ''), baseName);
    }

    fileRefs.set(
        baseName,
        extractReferencedSymbols(cleanCode, isHtml),
    );
  }

  const dependencyMap = {};

  // Pass 2: Map each file to its resolved dependency filenames
  for (const [baseName, refs] of fileRefs.entries()) {
    const deps = new Set();
    const localDeclared = fileDeclared.get(baseName) || new Set();
    for (const ref of refs) {
      // If the symbol is declared locally within the same file, do not treat as
      // an external dependency
      if (localDeclared.has(ref)) {
        continue;
      }
      if (symbolToFile.has(ref)) {
        const targetFile = symbolToFile.get(ref);
        if (targetFile !== baseName) {
          deps.add(targetFile);
        }
      }
    }
    if (deps.size > 0) {
      dependencyMap[baseName] = Array.from(deps).sort();
    }
  }

  // Inject default base files that are entry points and not depended on by
  // others
  dependencyMap['_default'] = ['trigger_functions.js'];

  return dependencyMap;
}

// CLI Execution entrypoint
if (process.argv[1] === __filename) {
  const isCheckMode = process.argv.includes('--check');
  const generatedGraph = generateDependencyGraph();
  const formattedJson = JSON.stringify(generatedGraph, null, 2) + '\n';

  if (isCheckMode) {
    if (!fs.existsSync(TARGET_JSON_PATH)) {
      console.error('[Dependency Generator] dependency.json missing!');
      process.exit(1);
    }
    const current = fs.readFileSync(TARGET_JSON_PATH, 'utf8');
    if (current !== formattedJson) {
      console.error(
          '[Dependency Generator] dependency.json is out-of-date! Run npm run deps:gen',
      );
      process.exit(1);
    }
    console.log('[Dependency Generator] dependency.json is up-to-date.');
  } else {
    fs.writeFileSync(TARGET_JSON_PATH, formattedJson, 'utf8');
    console.log(
        `[Dependency Generator] Generated updated dependency graph at ${
            TARGET_JSON_PATH}`,
    );
  }
}
