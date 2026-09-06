use std::collections::BTreeMap;

pub(crate) fn extract_with_options(sql: &str) -> BTreeMap<String, String> {
    let mut options = BTreeMap::new();
    let Some((start, end)) = find_trailing_with_span(sql) else {
        return options;
    };

    let with_clause = &sql[start..end];
    let Some(open_paren) = with_clause.find('(') else {
        return options;
    };

    let inside = with_clause[open_paren + 1..].trim().trim_end_matches(')').trim();

    for item in split_top_level(inside, ',') {
        let item = item.trim();

        if item.is_empty() {
            continue;
        }

        if let Some(eq_index) = find_top_level_char(item, '=') {
            let key = clean_identifier_token(item[..eq_index].trim()).to_ascii_uppercase();
            let value = item[eq_index + 1..].trim().to_string();
            options.insert(key, value);
        }
    }

    options
}

pub(crate) fn strip_trailing_with_options(sql: &str) -> String {
    match find_trailing_with_span(sql) {
        Some((start, _end)) => sql[..start].trim_end().to_string(),
        None => sql.to_string(),
    }
}

pub(crate) fn find_trailing_with_span(sql: &str) -> Option<(usize, usize)> {
    let words = word_spans(sql);

    for word in words.iter().rev() {
        if !eq_ci(word.text, "WITH") {
            continue;
        }

        let after_with = skip_ws(sql, word.end);

        if sql.as_bytes().get(after_with) != Some(&b'(') {
            continue;
        }

        if let Some(close) = find_matching_paren(sql, after_with) {
            let after_close = sql[close + 1..].trim();

            if after_close.is_empty() || after_close == ";" {
                return Some((word.start, close + 1));
            }
        }
    }

    None
}

fn find_matching_paren(sql: &str, open_index: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = sql.as_bytes();
    let mut i = open_index;

    while i < bytes.len() {
        let ch = bytes[i] as char;

        if ch == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }

        i += 1;
    }

    None
}

fn split_top_level(input: &str, separator: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for (i, ch) in input.char_indices() {
        if ch == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' && depth > 0 {
                depth -= 1;
            } else if ch == separator && depth == 0 {
                out.push(input[start..i].to_string());
                start = i + ch.len_utf8();
            }
        }
    }

    if start < input.len() {
        out.push(input[start..].to_string());
    }

    out
}

fn find_top_level_char(input: &str, target: char) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for (i, ch) in input.char_indices() {
        if ch == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' && depth > 0 {
                depth -= 1;
            } else if ch == target && depth == 0 {
                return Some(i);
            }
        }
    }

    None
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WordSpan<'a> {
    pub(crate) text:  &'a str,
    pub(crate) start: usize,
    pub(crate) end:   usize,
}

pub(crate) fn word_spans(input: &str) -> Vec<WordSpan<'_>> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;

    for (i, ch) in input.char_indices() {
        let is_word = ch.is_ascii_alphanumeric() || ch == '_' || ch == '.';

        match (start, is_word) {
            (None, true) => start = Some(i),
            (Some(s), false) => {
                words.push(WordSpan {
                    text:  &input[s..i],
                    start: s,
                    end:   i,
                });
                start = None;
            },
            _ => {},
        }
    }

    if let Some(s) = start {
        words.push(WordSpan {
            text:  &input[s..],
            start: s,
            end:   input.len(),
        });
    }

    words
}

pub(crate) fn skip_ws(input: &str, mut index: usize) -> usize {
    while let Some(byte) = input.as_bytes().get(index) {
        if !byte.is_ascii_whitespace() {
            break;
        }
        index += 1;
    }
    index
}

pub(crate) fn normalize_ident_key(value: &str) -> String {
    clean_identifier_token(value).to_ascii_lowercase()
}

pub(crate) fn normalize_object_key(value: &str) -> String {
    value
        .split('.')
        .map(|part| normalize_ident_key(part.trim()))
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn clean_identifier_token(value: &str) -> String {
    value
        .trim()
        .trim_end_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .trim()
        .to_string()
}

pub(crate) fn normalize_sql_fragment(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn eq_ci(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(crate) fn unquote(value: &str) -> String {
    value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .trim_matches('`')
        .trim()
        .to_string()
}

pub(crate) fn same_option_value(left: &str, right: &str) -> bool {
    let left = normalize_sql_fragment(left);
    let right = normalize_sql_fragment(right);

    if left == right {
        return true;
    }

    unquote(&left).eq_ignore_ascii_case(&unquote(&right))
}

pub(crate) fn trim_leading_sql_comments(mut input: &str) -> &str {
    loop {
        let trimmed = input.trim_start();

        if let Some(rest) = trimmed.strip_prefix("--") {
            let Some(newline_index) = rest.find('\n') else {
                return "";
            };

            input = &rest[newline_index + 1..];
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("/*") {
            let Some(comment_end) = rest.find("*/") else {
                return "";
            };

            input = &rest[comment_end + 2..];
            continue;
        }

        return trimmed;
    }
}

pub(crate) fn is_contract_ddl(sql: &str) -> bool {
    let sql = sql.trim_start();
    starts_ci(sql, "CREATE TYPE")
        || starts_ci(sql, "ALTER TYPE")
        || starts_ci(sql, "DROP TYPE")
        || starts_ci(sql, "CREATE PROCEDURE")
        || starts_ci(sql, "CREATE OR REPLACE PROCEDURE")
        || starts_ci(sql, "DROP PROCEDURE")
        || starts_ci(sql, "GRANT EXECUTE")
        || starts_ci(sql, "REVOKE EXECUTE")
        || starts_ci(sql, "SET SEARCH_PATH")
        || starts_ci(sql, "SET NAMESPACE")
        || starts_ci(sql, "USE ")
        || starts_ci(sql, "USE NAMESPACE")
}

fn starts_ci(sql: &str, prefix: &str) -> bool {
    sql.len() >= prefix.len()
        && sql.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// Remove `ROW TYPE ident` so sqlparser can parse CREATE TABLE.
pub(crate) fn strip_row_type_clause(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    while i + 8 < bytes.len() {
        let ch = bytes[i] as char;
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                let after = sql[i + 1..].trim_start();
                if after.len() >= 8 && after[..8].eq_ignore_ascii_case("ROW TYPE") {
                    let alias_start = sql.len() - after.len() + 8;
                    let rest = sql[alias_start..].trim_start();
                    let ident_len =
                        rest.find(|ch: char| ch.is_whitespace() || ch == '(').unwrap_or(rest.len());
                    let after_ident = rest[ident_len..].trim_start();
                    return format!("{} {}", sql[..=i].trim_end(), after_ident);
                }
            }
        }
        i += 1;
    }
    sql.to_string()
}
