// Named-query allowlist with typed, validated parameters — the security core
// of the BFF. The browser never sends SQL; it sends a query NAME and a bag of
// parameters. This module resolves the name against a server-defined registry
// and turns each parameter into a safe SQL literal.
//
// Injection-proof by construction:
//   - There is NO free-form string parameter type. Text filters must be an
//     `enum` whose allowed values are server-defined, so no untrusted string
//     is ever placed into SQL.
//   - int / number / bool are re-serialized from their coerced primitive, so
//     the emitted text cannot contain a SQL metacharacter.
//   - date is validated against a strict ISO regex (no quote/semicolon can
//     pass) before being wrapped in quotes.
// The query template receives ONLY these validated literals, never raw input.

/** A parameter's declared shape. `type` is required; the rest are per-type. */
// { type: "int",    min?, max?, default? }
// { type: "number", min?, max?, default? }
// { type: "bool",   default? }
// { type: "date",   default? }                     // 'YYYY-MM-DD' or ISO datetime
// { type: "enum",   values: [...], default? }      // membership-checked

const ISO_DATE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z?)?$/;

class ParamError extends Error {}

/** Validate one raw value against its schema; return a safe SQL literal string. */
function literalFor(name, spec, raw) {
  if (raw === undefined || raw === null) {
    if ("default" in spec) return literalFor(name, spec, spec.default);
    throw new ParamError(`missing required parameter "${name}"`);
  }
  switch (spec.type) {
    case "int": {
      const n = Number(raw);
      if (!Number.isInteger(n)) throw new ParamError(`"${name}" must be an integer`);
      if (spec.min != null && n < spec.min) throw new ParamError(`"${name}" below min ${spec.min}`);
      if (spec.max != null && n > spec.max) throw new ParamError(`"${name}" above max ${spec.max}`);
      return String(n);
    }
    case "number": {
      const n = Number(raw);
      if (!Number.isFinite(n)) throw new ParamError(`"${name}" must be a finite number`);
      if (spec.min != null && n < spec.min) throw new ParamError(`"${name}" below min ${spec.min}`);
      if (spec.max != null && n > spec.max) throw new ParamError(`"${name}" above max ${spec.max}`);
      return String(n);
    }
    case "bool": {
      if (typeof raw === "boolean") return raw ? "TRUE" : "FALSE";
      if (raw === "true" || raw === "false") return raw === "true" ? "TRUE" : "FALSE";
      throw new ParamError(`"${name}" must be a boolean`);
    }
    case "date": {
      if (typeof raw !== "string" || !ISO_DATE.test(raw)) {
        throw new ParamError(`"${name}" must be an ISO date/datetime`);
      }
      return `'${raw}'`; // regex forbids quote/semicolon, so this is safe
    }
    case "enum": {
      if (!Array.isArray(spec.values) || !spec.values.includes(raw)) {
        throw new ParamError(`"${name}" is not an allowed value`);
      }
      // The value came from the server-defined list, but re-check it is
      // quote-free before quoting a string (belt and suspenders).
      if (typeof raw === "number") return String(raw);
      if (typeof raw === "string" && !raw.includes("'")) return `'${raw}'`;
      throw new ParamError(`enum "${name}" has an unquotable configured value`);
    }
    default:
      throw new ParamError(`parameter "${name}" has unknown type "${spec.type}"`);
  }
}

/**
 * Resolve a request against the registry. Returns { sql } on success or
 * throws ParamError (bad params) / a plain Error (unknown query name).
 * @param {Record<string, {sql: (lits: Record<string,string>) => string,
 *   params?: Record<string, object>, description?: string}>} registry
 * @param {string} name
 * @param {Record<string, unknown>} rawParams
 */
export function resolveQuery(registry, name, rawParams = {}) {
  const entry = registry[name];
  if (!entry) {
    const e = new Error(`unknown query "${name}"`);
    e.code = "UNKNOWN_QUERY";
    throw e;
  }
  const specs = entry.params ?? {};
  // Reject any parameter the query does not declare (fail closed).
  for (const key of Object.keys(rawParams)) {
    if (!(key in specs)) throw new ParamError(`unexpected parameter "${key}"`);
  }
  const literals = {};
  for (const [key, spec] of Object.entries(specs)) {
    literals[key] = literalFor(key, spec, rawParams[key]);
  }
  const sql = entry.sql(literals);
  if (typeof sql !== "string" || !sql.trim()) {
    throw new Error(`query "${name}" produced no SQL`);
  }
  return { sql };
}

/** A safe public description of the registry (names + param schemas, no SQL). */
export function describeRegistry(registry) {
  const out = {};
  for (const [name, entry] of Object.entries(registry)) {
    out[name] = {
      description: entry.description ?? null,
      params: entry.params ?? {},
    };
  }
  return out;
}

export { ParamError };
