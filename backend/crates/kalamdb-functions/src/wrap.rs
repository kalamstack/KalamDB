//! Wrap CREATE PROCEDURE bodies into the V1 `kalamInvoke` ABI.

/// If the body already defines `kalamInvoke`, keep it. Otherwise wrap it as
/// `(ctx, input) => { body }` so dollar-quoted JS can use host objects.
pub fn wrap_procedure_source(body: &str) -> String {
    if body.contains("function kalamInvoke") {
        return body.to_string();
    }
    format!(
        "function kalamInvoke(name, args) {{\nconst ctx = globalThis.__kalamCtx;\nconst input = \
         args.length === 1 ? args[0] : Array.from(args);\nreturn (function (ctx, input) \
         {{\n{body}\n}})(ctx, input);\n}}\n"
    )
}

pub const HOST_BOOTSTRAP: &str = r#"
function __kalamMakeCtx() {
  var source = { kind: "call" };
  try {
    if (typeof kalamHostSource === "function") {
      source = kalamHostSource() || source;
    }
  } catch (e) {}
  var parent = null;
  try {
    if (typeof kalamHostParent === "function") {
      parent = kalamHostParent();
    }
  } catch (e) {}
  return {
    source: source,
    parent: parent,
    db: {
      sql: function (q) { return kalamHostSql(String(q)); }
    },
    functions: {
      call: function (name, args) { return kalamHostCall(String(name), args); }
    },
    topics: {
      publish: function (topic, payload) { kalamHostPublish(String(topic), payload); }
    },
    http: {
      request: {
        header: function (name) { return kalamHostHttpHeader(String(name)); }
      },
      status: function (code) { kalamHostHttpSetStatus(code); },
      header: function (name, value) { kalamHostHttpSetHeader(String(name), String(value)); }
    }
  };
}
"#;

/// ABI v2 operations always return Promises. Metadata comes only from the host.
pub const ASYNC_HOST_BOOTSTRAP: &str = r#"
function __kalamMakeCtx() {
  const metadata = JSON.parse(kalamHostMetadata());
  return Object.freeze({
    ...metadata,
    source: Object.freeze(kalamHostSource()),
    parent: kalamHostParent(),
    db: Object.freeze({
      query: (sql, params = []) => kalamAsyncOp('query', sql, params),
      execute: (sql, params = []) => kalamAsyncOp('execute', sql, params),
    }),
    functions: Object.freeze({call: (name, args = []) => kalamAsyncOp('call', name, args)}),
    topics: Object.freeze({publish: (topic, payload) => kalamAsyncOp('publish', topic, [payload])}),
    log: Object.freeze({
      debug: message => kalamHostLog('debug', String(message)),
      info: message => kalamHostLog('info', String(message)),
      warn: message => kalamHostLog('warn', String(message)),
      error: message => kalamHostLog('error', String(message)),
    }),
    http: Object.freeze({
      request: Object.freeze({header: name => kalamHostHttpHeader(String(name))}),
      status: code => kalamHostHttpSetStatus(code),
      header: (name, value) => kalamHostHttpSetHeader(String(name), String(value)),
    }),
  });
}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_leaves_kalam_invoke_alone() {
        let source = "function kalamInvoke(name, args) { return args[0]; }";
        assert_eq!(wrap_procedure_source(source), source);
    }

    #[test]
    fn wrap_dollar_quoted_body() {
        let wrapped = wrap_procedure_source("return ctx.db.sql('SELECT 1');");
        assert!(wrapped.contains("function kalamInvoke"));
        assert!(wrapped.contains("ctx.db.sql"));
    }
}
