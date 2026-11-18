// Minimal HCL block reader, shared by the structural tests in this directory.
//
// Enough to walk block bodies with their nesting; no expression evaluation. It is the
// same code the MySQL test has always used, moved here so the PostgreSQL and failover
// tests read the configuration the same way the MySQL one does.
//
//   import { blocks, block, assignment, listBody } from "./hcl.mjs";

import assert from "node:assert/strict";

export function endOfBody(text, openBrace) {
  let depth = 0;
  let inString = false;
  let escaped = false;
  let inLineComment = false;
  let inBlockComment = false;
  for (let i = openBrace; i < text.length; i += 1) {
    const c = text[i];
    const next = text[i + 1];
    if (inLineComment) {
      if (c === "\n") inLineComment = false;
      continue;
    }
    if (inBlockComment) {
      if (c === "*" && next === "/") {
        inBlockComment = false;
        i += 1;
      }
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') inString = false;
      continue;
    }
    if (c === "#" || (c === "/" && next === "/")) {
      inLineComment = true;
      continue;
    }
    if (c === "/" && next === "*") {
      inBlockComment = true;
      i += 1;
      continue;
    }
    if (c === '"') inString = true;
    else if (c === "{") depth += 1;
    else if (c === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  throw new Error("unbalanced braces");
}

// Returns the bodies (text between the braces) of every block whose header matches.
export function blocks(text, header) {
  const found = [];
  const re = new RegExp(`^\\s*${header}\\s*(?:"([^"]*)")?[^\\n{]*\\{`, "gm");
  let m;
  while ((m = re.exec(text)) !== null) {
    const open = text.indexOf("{", m.index + m[0].length - 1);
    const close = endOfBody(text, open);
    found.push({ body: text.slice(open + 1, close), labels: m[1] });
  }
  return found;
}

// Reads `name = [ ... ]` and returns the text between the brackets.
export function listBody(text, name) {
  const start = text.search(new RegExp(`^\\s*${name}\\s*=\\s*\\[`, "m"));
  assert.ok(start >= 0, `${name} = [ ... ] not found`);
  const open = text.indexOf("[", start);
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    if (text[i] === "[") depth += 1;
    else if (text[i] === "]") {
      depth -= 1;
      if (depth === 0) return text.slice(open + 1, i);
    }
  }
  throw new Error(`unbalanced brackets in ${name}`);
}

export const block = (text, header) => {
  const found = blocks(text, header);
  assert.equal(found.length, 1, `expected exactly one ${header} block`);
  return found[0].body;
};

export const assignment = (body, name) => {
  const m = body.match(new RegExp(`^\\s*${name}\\s*=\\s*(.+?)\\s*$`, "m"));
  if (!m) return undefined;
  return m[1].replace(/^"(.*)"$/, "$1");
};

// The kinds whose API has no labels field, so the run reaches them another way: either
// they are a child of something labelled, or the cleanup job finds them by the name
// prefix every one of them carries.
export const noLabelsField = new Set([
  "google_compute_network",
  "google_compute_subnetwork",
  "google_service_networking_connection",
  "google_sql_database",
]);

// Asserts every `resource` block in the given file texts is findable by the cleanup job:
// labelled if the API has a labels field, named under the run prefix if it does not.
// Returns how many resources it walked.
export function assertFindable(files) {
  const re = /resource\s+"([^"]+)"\s+"([^"]*)"[^\n{]*\{([\s\S]*?)\n\}/g;
  let resources = 0;
  for (const [file, text] of Object.entries(files)) {
    let m;
    while ((m = re.exec(text)) !== null) {
      const [, type, name, body] = m;
      resources += 1;
      if (noLabelsField.has(type)) {
        // ...and its name must still say which run it belongs to.
        const nameArg = assignment(body, "name");
        if (nameArg !== undefined && type !== "google_sql_database") {
          assert.match(nameArg, /local\.name_prefix/, `${file}: ${type}.${name} is named outside the run prefix`);
        }
        continue;
      }
      assert.match(
        body,
        /^\s*(user_)?labels\s*=\s*local\.common_labels\s*$/m,
        `${file}: ${type}.${name} is created without the run labels`,
      );
    }
  }
  return resources;
}
