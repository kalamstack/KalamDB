use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::comparison::{self, ComparisonTrend, PreviousRun};
use crate::config::Config;
use crate::metrics::{BenchmarkDetail, BenchmarkResult, SystemInfo};
use crate::verdict::{self, Verdict};

/// Write a self-contained HTML report (with embedded Chart.js) and return the file path.
pub fn write_html_report(
    results: &[BenchmarkResult],
    config: &Config,
    output_dir: &str,
    version: &str,
    previous: Option<&PreviousRun>,
    system: &SystemInfo,
    wall_clock_duration: Duration,
) -> Result<String, String> {
    fs::create_dir_all(output_dir).map_err(|e| format!("Failed to create output dir: {}", e))?;

    let timestamp = chrono::Utc::now();
    let version_slug = version.replace('.', "-").replace("-alpha", "-a").replace("-beta", "-b");
    let filename = format!("bench-{}-{}.html", timestamp.format("%Y-%m-%d-%H%M%S"), version_slug);
    let path = Path::new(output_dir).join(&filename);

    let html = build_html(
        results,
        config,
        &timestamp.to_rfc3339(),
        version,
        previous,
        system,
        wall_clock_duration,
    );
    fs::write(&path, html).map_err(|e| format!("Write error: {}", e))?;

    Ok(path.display().to_string())
}

/// Format microseconds into a human-readable string (µs / ms / s / m).
fn format_us(us: f64) -> String {
    format_duration_us(us)
}

/// Format ops/sec with SI suffixes.
fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K", ops / 1_000.0)
    } else {
        format!("{:.0}", ops)
    }
}

/// Format total time from µs into a readable string.
fn format_total(us: u64) -> String {
    format_duration_us(us as f64)
}

fn format_summary_seconds(seconds: f64) -> String {
    format!("{seconds:.3}s")
}

fn format_duration_us(us: f64) -> String {
    if us < 1000.0 {
        format!("{:.0}µs", us)
    } else if us < 10_000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else if us < 1_000_000.0 {
        format!("{:.1}ms", us / 1000.0)
    } else if us < 60_000_000.0 {
        format!("{:.3}s", us / 1_000_000.0)
    } else {
        format!("{:.2}m", us / 60_000_000.0)
    }
}

fn exact_duration_title_f64(us: f64) -> String {
    format!("{:.0}µs exact", us)
}

fn exact_duration_title_u64(us: u64) -> String {
    format!("{}µs exact", us)
}

fn exact_duration_title_u128(us: u128) -> String {
  format!("{}µs exact", us)
}

struct SummaryBreakdownItem<'a> {
  label: &'a str,
  value: String,
  tone_class: &'a str,
}

fn render_breakdown_card(title: &str, note: &str, items: &[SummaryBreakdownItem<'_>]) -> String {
  let items_html = items
    .iter()
    .map(|item| {
      format!(
        "<div class=\"summary-breakdown-item {}\"><span class=\"summary-breakdown-value\">{}</span><span class=\"summary-breakdown-label\">{}</span></div>",
        item.tone_class,
        html_escape(&item.value),
        html_escape(item.label),
      )
    })
    .collect::<Vec<_>>()
    .join("");

  format!(
    "<div class=\"card breakdown-card\"><div class=\"card-heading\">{}</div><div class=\"summary-breakdown\">{}</div><div class=\"card-note\">{}</div></div>",
    html_escape(title),
    items_html,
    html_escape(note),
  )
}

fn render_suite_total_row(measured_total_us: u64, wall_clock_duration: Duration) -> String {
  let measured_total = format_summary_seconds(measured_total_us as f64 / 1_000_000.0);
  let wall_clock_total = format_summary_seconds(wall_clock_duration.as_secs_f64());

  format!(
    "<tfoot><tr class=\"suite-total-row\"><td colspan=\"12\" class=\"suite-total-label\">Whole Bench Totals</td><td class=\"num suite-total-value\" title=\"{}\">{}</td><td colspan=\"2\" class=\"suite-total-meta\" title=\"{}\">Wall clock {}</td></tr></tfoot>",
    html_escape(&exact_duration_title_u64(measured_total_us)),
    measured_total,
    html_escape(&exact_duration_title_u128(wall_clock_duration.as_micros())),
    wall_clock_total,
  )
}

fn render_description_cell(result: &BenchmarkResult) -> String {
    let full_description = if result.full_description.is_empty() {
        result.description.as_str()
    } else {
        result.full_description.as_str()
    };
  let escaped_full_description = html_escape(full_description);

    format!(
    "<div class=\"desc-wrap\"><div class=\"desc-summary\" title=\"{}\" data-full-description=\"{}\" tabindex=\"0\">{}</div>{}</div>",
    escaped_full_description,
    escaped_full_description,
        html_escape(&result.description),
        render_details(&result.details)
    )
}

fn render_details(details: &[BenchmarkDetail]) -> String {
    if details.is_empty() {
        return String::new();
    }

    let items = details
    .iter()
    .map(|detail| {
      let label = html_escape(&detail.label);
      let value = html_escape(&detail.value);
      format!(
        "<span class=\"detail-pill\" title=\"{}: {}\"><span class=\"detail-label\">{}</span><span class=\"detail-value\">{}</span></span>",
        label, value, label, value
      )
    })
    .collect::<Vec<_>>()
    .join("");

    format!("<div class=\"detail-list\">{}</div>", items)
}

fn build_html(
    results: &[BenchmarkResult],
    config: &Config,
    timestamp: &str,
    version: &str,
    previous: Option<&PreviousRun>,
    system: &SystemInfo,
    wall_clock_duration: Duration,
) -> String {
    let passed = results.iter().filter(|r| r.success).count();
    let failed = results.iter().filter(|r| !r.success).count();
  let measured_total_us: u64 = results.iter().map(|r| r.total_us).sum();

  let mut excellent_count = 0usize;
  let mut acceptable_count = 0usize;
  let mut slow_count = 0usize;
  let mut faster_count = 0usize;
  let mut same_count = 0usize;
  let mut slower_count = 0usize;
  let mut unavailable_count = 0usize;

  for result in results {
    match verdict::evaluate(result) {
      Verdict::Excellent => excellent_count += 1,
      Verdict::Acceptable => acceptable_count += 1,
      Verdict::Slow => slow_count += 1,
      Verdict::Failed => {}
    }

    match previous.and_then(|prev| comparison::compare(result, prev)) {
      Some(comparison) => match comparison.trend() {
        ComparisonTrend::Faster => faster_count += 1,
        ComparisonTrend::Same => same_count += 1,
        ComparisonTrend::Slower => slower_count += 1,
      },
      None => unavailable_count += 1,
    }
  }

  let verdict_summary_card = render_breakdown_card(
    "Verdict Mix",
    "successful benchmarks only",
    &[
      SummaryBreakdownItem {
        label: "Excellent",
        value: excellent_count.to_string(),
        tone_class: "tone-good",
      },
      SummaryBreakdownItem {
        label: "Acceptable",
        value: acceptable_count.to_string(),
        tone_class: "tone-warn",
      },
      SummaryBreakdownItem {
        label: "Slow",
        value: slow_count.to_string(),
        tone_class: "tone-bad",
      },
    ],
  );

  let comparison_summary_card = if let Some(previous_run) = previous {
    let note = if unavailable_count > 0 {
      format!(
        "against {} • {} unavailable/new",
        previous_run.timestamp, unavailable_count
      )
    } else {
      format!("against {}", previous_run.timestamp)
    };

    render_breakdown_card(
      "Compared To Previous",
      &note,
      &[
        SummaryBreakdownItem {
          label: "Faster",
          value: faster_count.to_string(),
          tone_class: "tone-good",
        },
        SummaryBreakdownItem {
          label: "Same",
          value: same_count.to_string(),
          tone_class: "tone-muted",
        },
        SummaryBreakdownItem {
          label: "Slower",
          value: slower_count.to_string(),
          tone_class: "tone-bad",
        },
      ],
    )
  } else {
    render_breakdown_card(
      "Compared To Previous",
      "no previous report available",
      &[
        SummaryBreakdownItem {
          label: "Faster",
          value: "—".to_string(),
          tone_class: "tone-muted",
        },
        SummaryBreakdownItem {
          label: "Same",
          value: "—".to_string(),
          tone_class: "tone-muted",
        },
        SummaryBreakdownItem {
          label: "Slower",
          value: "—".to_string(),
          tone_class: "tone-muted",
        },
      ],
    )
  };

  let suite_total_row = render_suite_total_row(measured_total_us, wall_clock_duration);

    // Group results by category
    let mut categories: Vec<String> = Vec::new();
    for r in results {
        if !categories.contains(&r.category) {
            categories.push(r.category.clone());
        }
    }

    // Build table rows
    let mut table_rows = String::new();
    for r in results {
        let status = if r.success {
            "<span class=\"badge badge-pass\">PASS</span>"
        } else {
            "<span class=\"badge badge-fail\">FAIL</span>"
        };
        let error_info = r
            .error
            .as_ref()
            .map(|e| format!("<br><small class=\"error-msg\">{}</small>", html_escape(e)))
            .unwrap_or_default();
        let v = verdict::evaluate(r);
        let delta_html = previous
            .and_then(|prev| comparison::compare(r, prev))
            .map(|c| c.html())
            .unwrap_or_else(|| "<span class=\"delta delta-none\">—</span>".to_string());
        let description_cell = render_description_cell(r);
        table_rows.push_str(&format!(
            "<tr>\
                <td>{status}</td>\
                <td><strong>{name}</strong>{error_info}</td>\
                <td><span class=\"cat-badge\">{category}</span></td>\
            <td class=\"desc-col\">{description}</td>\
                <td class=\"num\">{iterations}</td>\
            <td class=\"num\" title=\"{mean_title}\">{mean}</td>\
            <td class=\"num\" title=\"{median_title}\">{median}</td>\
            <td class=\"num\" title=\"{p95_title}\">{p95}</td>\
            <td class=\"num\" title=\"{p99_title}\">{p99}</td>\
            <td class=\"num\" title=\"{min_title}\">{min}</td>\
            <td class=\"num\" title=\"{max_title}\">{max}</td>\
                <td class=\"num ops\">{ops}</td>\
            <td class=\"num\" title=\"{total_title}\">{total}</td>\
                <td>{verdict}</td>\
                <td>{delta}</td>\
            </tr>",
            status = status,
            name = html_escape(&r.name),
            error_info = error_info,
            category = html_escape(&r.category),
            description = description_cell,
            iterations = r.iterations,
            mean = format_us(r.mean_us),
            median = format_us(r.median_us),
            p95 = format_us(r.p95_us),
            p99 = format_us(r.p99_us),
            min = format_us(r.min_us),
            max = format_us(r.max_us),
            mean_title = html_escape(&exact_duration_title_f64(r.mean_us)),
            median_title = html_escape(&exact_duration_title_f64(r.median_us)),
            p95_title = html_escape(&exact_duration_title_f64(r.p95_us)),
            p99_title = html_escape(&exact_duration_title_f64(r.p99_us)),
            min_title = html_escape(&exact_duration_title_f64(r.min_us)),
            max_title = html_escape(&exact_duration_title_f64(r.max_us)),
            ops = format_ops(r.ops_per_sec),
            total = format_total(r.total_us),
            total_title = html_escape(&exact_duration_title_u64(r.total_us)),
            verdict = v.html_badge(),
            delta = delta_html,
        ));
    }

    // JSON data for charts
    let chart_labels: Vec<String> = results.iter().map(|r| format!("\"{}\"", r.name)).collect();
    let chart_mean: Vec<String> = results.iter().map(|r| format!("{:.1}", r.mean_us)).collect();
    let chart_p95: Vec<String> = results.iter().map(|r| format!("{:.1}", r.p95_us)).collect();
    let chart_p99: Vec<String> = results.iter().map(|r| format!("{:.1}", r.p99_us)).collect();
    let chart_ops: Vec<String> = results.iter().map(|r| format!("{:.1}", r.ops_per_sec)).collect();

    // Category breakdown for pie chart
    let mut cat_ops: Vec<(String, f64)> = Vec::new();
    for cat in &categories {
        let avg: f64 = results
            .iter()
            .filter(|r| &r.category == cat && r.success)
            .map(|r| r.mean_us)
            .sum::<f64>()
            / results.iter().filter(|r| &r.category == cat && r.success).count().max(1) as f64;
        cat_ops.push((cat.clone(), avg));
    }
    let pie_labels: Vec<String> = cat_ops.iter().map(|(c, _)| format!("\"{}\"", c)).collect();
    let pie_values: Vec<String> = cat_ops.iter().map(|(_, v)| format!("{:.1}", v)).collect();

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KalamDB Benchmark Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0d1017;
    --surface: #151922;
    --surface2: #1c2130;
    --surface3: #232839;
    --border: #2a3044;
    --border-light: #353b52;
    --text: #e6e9f0;
    --text2: #8891a8;
    --text3: #5c6580;
    --accent: #6366f1;
    --accent2: #818cf8;
    --accent-glow: rgba(99,102,241,0.12);
    --green: #22c55e;
    --green-bg: rgba(34,197,94,0.08);
    --red: #ef4444;
    --red-bg: rgba(239,68,68,0.08);
    --orange: #f59e0b;
    --blue: #3b82f6;
    --purple: #a855f7;
    --cyan: #06b6d4;
    --radius: 10px;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.65;
    font-size: 15px;
    -webkit-font-smoothing: antialiased;
  }}
  .page {{ width: 100%; padding: 2rem 2.5rem; }}

  /* ── Header ── */
  header {{
    text-align: center;
    padding: 2.5rem 0 2rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 2rem;
  }}
  header h1 {{
    font-size: 2.2rem;
    font-weight: 800;
    letter-spacing: -0.02em;
    background: linear-gradient(135deg, var(--accent), var(--purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.4rem;
  }}
  header .subtitle {{ color: var(--text2); font-size: 0.92rem; }}
  .meta {{
    display: flex;
    gap: 0.75rem;
    justify-content: center;
    margin-top: 1rem;
    flex-wrap: wrap;
  }}
  .meta span {{
    background: var(--surface);
    padding: 0.35rem 0.9rem;
    border-radius: 20px;
    font-size: 0.8rem;
    border: 1px solid var(--border);
    color: var(--text2);
  }}

  /* ── Summary cards ── */
  .summary-cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
  }}
  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.4rem;
    text-align: center;
    transition: border-color .2s;
  }}
  .card:hover {{ border-color: var(--border-light); }}
  .card .value {{
    font-size: 2rem;
    font-weight: 700;
    color: var(--accent2);
    letter-spacing: -0.02em;
  }}
  .card .label {{ color: var(--text2); font-size: 0.82rem; margin-top: 0.3rem; }}
  .card.pass .value {{ color: var(--green); }}
  .card.fail .value {{ color: var(--red); }}
  .breakdown-card {{
    text-align: left;
    padding: 1.15rem 1.2rem;
  }}
  .card-heading {{
    color: var(--text);
    font-size: 0.82rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  .summary-breakdown {{
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.55rem;
    margin-top: 0.8rem;
  }}
  .summary-breakdown-item {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.55rem 0.65rem;
  }}
  .summary-breakdown-value {{
    display: block;
    color: var(--text);
    font-size: 1.3rem;
    font-weight: 700;
    line-height: 1.1;
  }}
  .summary-breakdown-label {{
    display: block;
    color: var(--text3);
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-top: 0.24rem;
  }}
  .summary-breakdown-item.tone-good .summary-breakdown-value {{ color: var(--green); }}
  .summary-breakdown-item.tone-warn .summary-breakdown-value {{ color: var(--orange); }}
  .summary-breakdown-item.tone-bad .summary-breakdown-value {{ color: var(--red); }}
  .summary-breakdown-item.tone-muted .summary-breakdown-value {{ color: var(--text2); }}
  .card-note {{
    color: var(--text3);
    font-size: 0.74rem;
    margin-top: 0.65rem;
  }}

  .sys-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.2rem;
    margin-bottom: 1.5rem;
  }}
  .sys-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0.8rem 1.1rem;
  }}
  .sys-item {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.55rem 0.75rem;
  }}
  .sys-item .k {{
    color: var(--text3);
    font-size: 0.73rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.2rem;
  }}
  .sys-item .v {{
    color: var(--text);
    font-size: 0.88rem;
    font-weight: 500;
    word-break: break-word;
  }}

  /* ── Charts ── */
  .charts {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.25rem;
    margin-bottom: 2rem;
  }}
  .chart-box {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.4rem;
  }}
  .chart-box h3 {{
    margin-bottom: 1rem;
    font-size: 1rem;
    font-weight: 600;
    color: var(--text);
  }}
  .chart-box.wide {{ grid-column: 1 / -1; }}

  /* ── Table ── */
  .section-title {{
    font-size: 1.15rem;
    font-weight: 700;
    margin-bottom: 1rem;
    color: var(--text);
  }}
  .table-wrap {{
    overflow-x: auto;
    border-radius: var(--radius);
    border: 1px solid var(--border);
    margin-bottom: 2rem;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    background: var(--surface);
    font-size: 0.88rem;
  }}
  th {{
    background: var(--surface2);
    padding: 0.75rem 0.9rem;
    text-align: left;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text3);
    font-weight: 600;
    white-space: nowrap;
    position: sticky;
    top: 0;
    border-bottom: 2px solid var(--border);
    border-right: 1px solid var(--border);
  }}
  th:last-child {{ border-right: none; }}
  td {{
    padding: 0.65rem 0.9rem;
    border-top: 1px solid var(--border);
    border-right: 1px solid var(--border);
    vertical-align: middle;
  }}
  td:last-child {{ border-right: none; }}
  td.num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
    font-family: 'JetBrains Mono', 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
    font-size: 0.84rem;
    color: var(--text);
    white-space: nowrap;
  }}
  td.num[title] {{ cursor: help; }}
  td.ops {{ color: var(--cyan); font-weight: 600; }}
  td.desc-col {{
    color: var(--text2);
    max-width: 420px;
    min-width: 320px;
    white-space: normal;
  }}
  .desc-wrap {{
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
  }}
  .desc-summary {{
    color: var(--text2);
    line-height: 1.45;
    cursor: help;
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 3;
    overflow: hidden;
    text-decoration: underline dotted transparent;
    text-underline-offset: 0.18rem;
  }}
  .desc-summary:hover {{ text-decoration-color: var(--text3); }}
  .desc-summary:focus-visible {{
    outline: 1px solid rgba(129,140,248,0.5);
    outline-offset: 3px;
    text-decoration-color: var(--text3);
  }}
  .detail-list {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }}
  .detail-pill {{
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.18rem 0.5rem;
    border-radius: 999px;
    background: rgba(129,140,248,0.08);
    border: 1px solid rgba(129,140,248,0.16);
    color: var(--text2);
    font-size: 0.72rem;
    line-height: 1.2;
    white-space: nowrap;
  }}
  .detail-label {{ color: var(--text3); }}
  .detail-value {{ color: var(--text); }}
  .hover-tooltip {{
    position: fixed;
    left: 0;
    top: 0;
    z-index: 9999;
    max-width: min(34rem, calc(100vw - 2rem));
    padding: 0.75rem 0.9rem;
    border-radius: 10px;
    background: rgba(28,33,48,0.98);
    border: 1px solid var(--border-light);
    color: var(--text);
    box-shadow: 0 18px 38px rgba(0,0,0,0.4);
    font-size: 0.82rem;
    line-height: 1.45;
    white-space: normal;
    pointer-events: none;
    opacity: 0;
    transform: translateY(4px);
    transition: opacity .14s ease, transform .14s ease;
  }}
  .hover-tooltip.is-visible {{
    opacity: 1;
    transform: translateY(0);
  }}
  tr:hover td {{ background: var(--surface2); }}
  tbody tr:nth-child(even) td {{ background: rgba(255,255,255,0.012); }}
  tbody tr:nth-child(even):hover td {{ background: var(--surface2); }}

  /* ── Badges ── */
  .badge {{
    display: inline-block;
    padding: 0.2rem 0.65rem;
    border-radius: 6px;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.04em;
  }}
  .badge-pass {{ background: var(--green-bg); color: var(--green); border: 1px solid rgba(34,197,94,0.2); }}
  .badge-fail {{ background: var(--red-bg); color: var(--red); border: 1px solid rgba(239,68,68,0.2); }}
  .cat-badge {{
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 6px;
    font-size: 0.72rem;
    font-weight: 500;
    background: var(--accent-glow);
    color: var(--accent2);
  }}
  .error-msg {{ color: var(--red); font-size: 0.78rem; }}

  /* ── Verdict badges ── */
  .verdict {{
    display: inline-block;
    padding: 0.2rem 0.65rem;
    border-radius: 6px;
    font-size: 0.72rem;
    font-weight: 600;
    white-space: nowrap;
  }}
  .verdict-excellent {{ background: var(--green-bg); color: var(--green); border: 1px solid rgba(34,197,94,0.25); }}
  .verdict-acceptable {{ background: rgba(245,158,11,0.08); color: var(--orange); border: 1px solid rgba(245,158,11,0.25); }}
  .verdict-slow {{ background: var(--red-bg); color: var(--red); border: 1px solid rgba(239,68,68,0.25); }}
  .verdict-failed {{ background: var(--red-bg); color: var(--red); border: 1px solid rgba(239,68,68,0.25); }}

  /* ── Comparison delta ── */
  .delta {{
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 6px;
    font-size: 0.72rem;
    font-weight: 500;
    white-space: nowrap;
  }}
  .delta-faster {{ background: var(--green-bg); color: var(--green); }}
  .delta-slower {{ background: var(--red-bg); color: var(--red); }}
  .delta-same {{ color: var(--text3); }}
  .delta-none {{ color: var(--text3); }}
  tfoot td {{
    background: var(--surface2);
    border-top: 2px solid var(--border-light);
    font-weight: 600;
  }}
  .suite-total-label {{
    color: var(--text);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-size: 0.76rem;
  }}
  .suite-total-value {{
    color: var(--accent2);
    font-size: 0.92rem;
  }}
  .suite-total-meta {{
    color: var(--text2);
    font-size: 0.8rem;
    white-space: nowrap;
  }}

  footer {{
    text-align: center;
    padding: 1.5rem 0;
    color: var(--text3);
    font-size: 0.78rem;
    border-top: 1px solid var(--border);
    margin-top: 1rem;
  }}

  @media (max-width: 1000px) {{
    .page {{ padding: 1rem; }}
    .charts {{ grid-template-columns: 1fr; }}
    .chart-box.wide {{ grid-column: 1; }}
    .summary-breakdown {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>
<div class="page">

<header>
  <h1>KalamDB Benchmark Report</h1>
  <div class="subtitle">Performance analysis &mdash; v{version}</div>
  <div class="meta">
    <span>{server_url}</span>
    <span>{date}</span>
    <span>{iterations} iters</span>
    <span>{warmup} warmup</span>
    <span>{concurrency} concurrency</span>
    <span>{max_subscribers} max subs</span>
  </div>
</header>

<div class="summary-cards">
  <div class="card">
    <div class="value">{total}</div>
    <div class="label">Total Benchmarks</div>
  </div>
  <div class="card pass">
    <div class="value">{passed}</div>
    <div class="label">Passed</div>
  </div>
  <div class="card fail">
    <div class="value">{failed}</div>
    <div class="label">Failed</div>
  </div>
  {verdict_summary_card}
  {comparison_summary_card}
  <div class="card">
    <div class="value">{measured_total}</div>
    <div class="label">Measured Benchmark Time</div>
  </div>
  <div class="card">
    <div class="value">{wall_clock_total}</div>
    <div class="label">Wall Clock Duration</div>
  </div>
</div>

<div class="sys-panel">
  <div class="section-title">System Information</div>
  <div class="sys-grid">
    <div class="sys-item"><div class="k">Hostname</div><div class="v">{sys_hostname}</div></div>
    <div class="sys-item"><div class="k">Machine Model</div><div class="v">{sys_model}</div></div>
    <div class="sys-item"><div class="k">CPU Model</div><div class="v">{sys_cpu_model}</div></div>
    <div class="sys-item"><div class="k">CPU Cores</div><div class="v">{sys_cpu_cores}</div></div>
    <div class="sys-item"><div class="k">Total Memory</div><div class="v">{sys_mem_total}</div></div>
    <div class="sys-item"><div class="k">Available Memory</div><div class="v">{sys_mem_available}</div></div>
    <div class="sys-item"><div class="k">Used Memory</div><div class="v">{sys_mem_used}</div></div>
    <div class="sys-item"><div class="k">Memory Usage</div><div class="v">{sys_mem_pct}</div></div>
    <div class="sys-item"><div class="k">OS</div><div class="v">{sys_os}</div></div>
    <div class="sys-item"><div class="k">Kernel</div><div class="v">{sys_kernel}</div></div>
    <div class="sys-item"><div class="k">Architecture</div><div class="v">{sys_arch}</div></div>
  </div>
</div>

<div class="charts">
  <div class="chart-box wide">
    <h3>Latency by Operation (µs)</h3>
    <canvas id="latencyChart" height="70"></canvas>
  </div>
  <div class="chart-box">
    <h3>Throughput (ops/sec)</h3>
    <canvas id="throughputChart" height="120"></canvas>
  </div>
  <div class="chart-box">
    <h3>Avg Latency by Category (µs)</h3>
    <canvas id="categoryChart" height="120"></canvas>
  </div>
</div>

<div class="section-title">Detailed Results</div>
<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th>Status</th>
      <th>Benchmark</th>
      <th>Category</th>
      <th>Description</th>
      <th style="text-align:right">Iters</th>
      <th style="text-align:right">Mean</th>
      <th style="text-align:right">P50</th>
      <th style="text-align:right">P95</th>
      <th style="text-align:right">P99</th>
      <th style="text-align:right">Min</th>
      <th style="text-align:right">Max</th>
      <th style="text-align:right">Ops/sec</th>
      <th style="text-align:right">Total</th>
      <th>Verdict</th>
      <th>vs Prev</th>
    </tr>
  </thead>
  <tbody>
    {table_rows}
  </tbody>
  {suite_total_row}
</table>
</div>

<div id="descriptionTooltip" class="hover-tooltip" hidden></div>

<footer>
  KalamDB v{version} &mdash; Generated {date}
</footer>

</div>

<script>
Chart.defaults.color = '#8891a8';
Chart.defaults.borderColor = '#2a3044';
Chart.defaults.font.family = "'Inter', -apple-system, sans-serif";
const COLORS = ['#6366f1','#22c55e','#f59e0b','#3b82f6','#ef4444','#a855f7','#ec4899','#06b6d4','#f97316','#84cc16','#64748b'];

// Latency chart
new Chart(document.getElementById('latencyChart'), {{
  type: 'bar',
  data: {{
    labels: [{labels}],
    datasets: [
      {{ label: 'Mean', data: [{mean}], backgroundColor: '#6366f1cc', borderRadius: 3 }},
      {{ label: 'P95',  data: [{p95}],  backgroundColor: '#f59e0bcc', borderRadius: 3 }},
      {{ label: 'P99',  data: [{p99}],  backgroundColor: '#ef4444cc', borderRadius: 3 }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ position: 'top' }} }},
    scales: {{
      y: {{ beginAtZero: true, title: {{ display: true, text: 'Latency (µs)' }}, grid: {{ color: '#1c2130' }} }},
      x: {{ ticks: {{ maxRotation: 45 }}, grid: {{ color: '#1c2130' }} }}
    }}
  }}
}});

// Throughput chart
new Chart(document.getElementById('throughputChart'), {{
  type: 'bar',
  data: {{
    labels: [{labels}],
    datasets: [{{ label: 'Ops/sec', data: [{ops}], backgroundColor: COLORS, borderRadius: 3 }}]
  }},
  options: {{
    indexAxis: 'y',
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ beginAtZero: true, title: {{ display: true, text: 'Operations / second' }}, grid: {{ color: '#1c2130' }} }},
      y: {{ grid: {{ display: false }} }}
    }}
  }}
}});

// Category chart
new Chart(document.getElementById('categoryChart'), {{
  type: 'doughnut',
  data: {{
    labels: [{pie_labels}],
    datasets: [{{ data: [{pie_values}], backgroundColor: COLORS, borderWidth: 0 }}]
  }},
  options: {{
    responsive: true,
    cutout: '55%',
    plugins: {{
      legend: {{ position: 'right' }},
      tooltip: {{ callbacks: {{ label: (ctx) => ctx.label + ': ' + ctx.parsed.toFixed(0) + ' µs avg' }} }}
    }}
  }}
}});

const descriptionTooltip = document.getElementById('descriptionTooltip');
const descriptionTriggers = document.querySelectorAll('.desc-summary[data-full-description]');

function positionDescriptionTooltip(left, top) {{
  if (!descriptionTooltip || descriptionTooltip.hidden) {{
    return;
  }}

  const margin = 16;
  const tooltipWidth = descriptionTooltip.offsetWidth;
  const tooltipHeight = descriptionTooltip.offsetHeight;
  let nextLeft = left;
  let nextTop = top;

  if (nextLeft + tooltipWidth > window.innerWidth - margin) {{
    nextLeft = window.innerWidth - tooltipWidth - margin;
  }}

  if (nextTop + tooltipHeight > window.innerHeight - margin) {{
    nextTop = top - tooltipHeight - 18;
  }}

  if (nextLeft < margin) {{
    nextLeft = margin;
  }}

  if (nextTop < margin) {{
    nextTop = margin;
  }}

  descriptionTooltip.style.left = `${{nextLeft}}px`;
  descriptionTooltip.style.top = `${{nextTop}}px`;
}}

function showDescriptionTooltip(trigger, left, top) {{
  if (!descriptionTooltip) {{
    return;
  }}

  const text = trigger.dataset.fullDescription;
  if (!text) {{
    return;
  }}

  descriptionTooltip.textContent = text;
  descriptionTooltip.hidden = false;
  descriptionTooltip.classList.add('is-visible');
  positionDescriptionTooltip(left, top);
}}

function hideDescriptionTooltip() {{
  if (!descriptionTooltip) {{
    return;
  }}

  descriptionTooltip.classList.remove('is-visible');
  descriptionTooltip.hidden = true;
}}

descriptionTriggers.forEach((trigger) => {{
  trigger.addEventListener('mouseenter', (event) => {{
    showDescriptionTooltip(trigger, event.clientX + 14, event.clientY + 18);
  }});

  trigger.addEventListener('mousemove', (event) => {{
    positionDescriptionTooltip(event.clientX + 14, event.clientY + 18);
  }});

  trigger.addEventListener('mouseleave', hideDescriptionTooltip);

  trigger.addEventListener('focus', () => {{
    const rect = trigger.getBoundingClientRect();
    showDescriptionTooltip(trigger, rect.left, rect.bottom + 12);
  }});

  trigger.addEventListener('blur', hideDescriptionTooltip);
}});

window.addEventListener('scroll', hideDescriptionTooltip, true);
window.addEventListener('resize', hideDescriptionTooltip);
</script>
</body>
</html>
"##,
        server_url = html_escape(&config.urls.join(", ")),
        date = timestamp,
        iterations = config.iterations,
        warmup = config.warmup,
        concurrency = config.concurrency,
        max_subscribers = format_total_count(config.max_subscribers),
        total = results.len(),
        passed = passed,
        failed = failed,
        verdict_summary_card = verdict_summary_card,
        comparison_summary_card = comparison_summary_card,
        measured_total = format_summary_seconds(measured_total_us as f64 / 1_000_000.0),
        wall_clock_total = format_summary_seconds(wall_clock_duration.as_secs_f64()),
        table_rows = table_rows,
        suite_total_row = suite_total_row,
        version = version,
        labels = chart_labels.join(","),
        mean = chart_mean.join(","),
        p95 = chart_p95.join(","),
        p99 = chart_p99.join(","),
        ops = chart_ops.join(","),
        pie_labels = pie_labels.join(","),
        pie_values = pie_values.join(","),
        sys_hostname = html_escape(&system.hostname),
        sys_model = html_escape(&system.machine_model),
        sys_cpu_model = html_escape(&system.cpu_model),
        sys_cpu_cores = format!(
            "{} logical / {} physical",
            system.cpu_logical_cores, system.cpu_physical_cores
        ),
        sys_mem_total = format_bytes(system.total_memory_bytes),
        sys_mem_available = format_bytes(system.available_memory_bytes),
        sys_mem_used = format_bytes(system.used_memory_bytes),
        sys_mem_pct = format!("{:.1}%", system.used_memory_percent),
        sys_os = html_escape(&format!("{} {}", system.os_name, system.os_version)),
        sys_kernel = html_escape(&system.kernel_version),
        sys_arch = html_escape(&system.architecture),
    )
}

fn format_total_count(value: u32) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;

    let b = bytes as f64;
    if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else {
        format!("{:.1} MiB", b / MIB)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
  use std::collections::HashMap;

  use crate::comparison::{PreviousBenchmark, PreviousRun};

    #[test]
    fn build_html_renders_full_description_details_and_exact_time_tooltips() {
        let config = Config {
            urls: vec!["http://127.0.0.1:8080".to_string()],
            user: "admin".to_string(),
            password: "kalamdb123".to_string(),
            iterations: 1,
            warmup: 0,
            concurrency: 1,
            output_dir: "results".to_string(),
            filter: Some("subscriber_scale".to_string()),
            bench: vec!["subscriber_scale".to_string()],
            list_benches: false,
            namespace: "bench_test".to_string(),
            max_subscribers: 100_000,
        };
        let result = BenchmarkResult::from_durations(
            "subscriber_scale",
            "Scale",
            "Progressive live-query subscriber scale and insert fanout verification up to 100.0K",
            vec![Duration::from_micros(37_004_345)],
        )
        .with_report_context(
            "Full subscriber-scale verification description",
            vec![
                BenchmarkDetail::new("Batch/Wave", "1.0K / 500"),
                BenchmarkDetail::new("Delivery Checks", "all tiers <=10.0K + 25.0K/50.0K/100.0K"),
            ],
        );

        let html = build_html(
            &[result],
            &config,
            "2026-04-19T00:00:00Z",
            "0.4.2-rc.3",
            None,
            &SystemInfo::default(),
            Duration::from_micros(38_000_000),
        );

        assert!(html.contains("Full subscriber-scale verification description"));
        assert!(html.contains("Batch/Wave"));
        assert!(html.contains("data-full-description=\"Full subscriber-scale verification description\""));
        assert!(html.contains("37.004s"));
        assert!(html.contains("38.000s"));
        assert!(html.contains("37004345µs exact"));
        assert!(html.contains("Measured Benchmark Time"));
        assert!(html.contains("Wall Clock Duration"));
        assert!(html.contains("Whole Bench Totals"));
        assert!(html.contains("Wall clock 38.000s"));
        assert!(html.contains("Compared To Previous"));
    }

      #[test]
      fn build_html_renders_verdict_and_previous_run_summary_cards() {
        let config = Config {
          urls: vec!["http://127.0.0.1:8080".to_string()],
          user: "admin".to_string(),
          password: "kalamdb123".to_string(),
          iterations: 1,
          warmup: 0,
          concurrency: 1,
          output_dir: "results".to_string(),
          filter: None,
          bench: vec![],
          list_benches: false,
          namespace: "bench_test".to_string(),
          max_subscribers: 100_000,
        };

        let excellent = BenchmarkResult::from_durations(
          "subscriber_scale",
          "Scale",
          "subscriber scale",
          vec![Duration::from_micros(37_004_345)],
        );
        let acceptable = BenchmarkResult::from_durations(
          "single_insert",
          "Insert",
          "single insert",
          vec![Duration::from_micros(3_000)],
        );
        let slow = BenchmarkResult::from_durations(
          "concurrent_select",
          "Concurrent",
          "concurrent select",
          vec![Duration::from_micros(80_000)],
        );

        let previous = PreviousRun {
          timestamp: "2026-04-18T12:00:00Z".to_string(),
          results: HashMap::from([
            (
              "subscriber_scale".to_string(),
              PreviousBenchmark {
                mean_us: 40_000_000.0,
                median_us: 40_000_000.0,
                p95_us: 40_000_000.0,
                p99_us: 40_000_000.0,
                ops_per_sec: 1.0,
              },
            ),
            (
              "single_insert".to_string(),
              PreviousBenchmark {
                mean_us: 3_050.0,
                median_us: 3_050.0,
                p95_us: 3_050.0,
                p99_us: 3_050.0,
                ops_per_sec: 1.0,
              },
            ),
            (
              "concurrent_select".to_string(),
              PreviousBenchmark {
                mean_us: 60_000.0,
                median_us: 60_000.0,
                p95_us: 60_000.0,
                p99_us: 60_000.0,
                ops_per_sec: 1.0,
              },
            ),
          ]),
        };

        let html = build_html(
          &[excellent, acceptable, slow],
          &config,
          "2026-04-19T00:00:00Z",
          "0.4.2-rc.3",
          Some(&previous),
          &SystemInfo::default(),
          Duration::from_secs(50),
        );

        assert!(html.contains("Verdict Mix"));
        assert!(html.contains("Compared To Previous"));
        assert!(html.contains("against 2026-04-18T12:00:00Z"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Excellent</span>"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Acceptable</span>"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Slow</span>"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Faster</span>"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Same</span>"));
        assert!(html.contains("<span class=\"summary-breakdown-value\">1</span><span class=\"summary-breakdown-label\">Slower</span>"));
      }
}
