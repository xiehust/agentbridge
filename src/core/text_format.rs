//! Chat-friendly text rendering helpers.
//!
//! Chat platforms (Discord, Telegram) do not render GitHub-flavored Markdown
//! tables — the `| --- |` pipes show literally and misalign on a narrow phone
//! screen. [`tables_to_cards`] rewrites any Markdown table into a per-row
//! "card": the first column becomes a bold heading and the remaining columns
//! become `· Header: value` bullets, which reads cleanly on mobile.

/// Rewrite every Markdown table in `text` into mobile-friendly cards, leaving
/// all non-table text untouched. Idempotent on text with no tables.
///
/// A table is detected as: a header row containing `|`, immediately followed by
/// a separator row whose cells are all dashes (`---`, `:--:`, …), followed by
/// zero or more body rows. Anything that doesn't match that exact shape is left
/// as-is, so a stray `|` in prose or code is never mangled.
pub fn tables_to_cards(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        // A table needs a header line + a separator line right after it.
        if i + 1 < lines.len() && is_table_row(lines[i]) && is_separator_row(lines[i + 1]) {
            let headers = split_row(lines[i]);
            let mut j = i + 2;
            let mut rows: Vec<Vec<String>> = Vec::new();
            while j < lines.len() && is_table_row(lines[j]) && !is_separator_row(lines[j]) {
                rows.push(split_row(lines[j]));
                j += 1;
            }
            // Only treat it as a table if it actually has body rows; otherwise
            // it's probably not a real table — leave the lines untouched.
            if !rows.is_empty() {
                render_cards(&headers, &rows, &mut out);
                i = j;
                continue;
            }
        }
        out.push(lines[i].to_string());
        i += 1;
    }

    out.join("\n")
}

/// A line that looks like a table row: trimmed, contains `|`, and isn't empty.
fn is_table_row(line: &str) -> bool {
    let t = line.trim();
    !t.is_empty() && t.contains('|')
}

/// The `|---|:--:|` separator row: every cell is dashes (optionally colon-anchored).
fn is_separator_row(line: &str) -> bool {
    let cells = split_row(line);
    if cells.is_empty() {
        return false;
    }
    cells.iter().all(|c| {
        let c = c.trim();
        !c.is_empty()
            && c.chars().all(|ch| ch == '-' || ch == ':')
            && c.contains('-')
    })
}

/// Split a Markdown table row into trimmed cells, dropping the empty leading /
/// trailing cells produced by the bordering `|`.
fn split_row(line: &str) -> Vec<String> {
    let t = line.trim();
    let t = t.strip_prefix('|').unwrap_or(t);
    let t = t.strip_suffix('|').unwrap_or(t);
    t.split('|').map(|c| c.trim().to_string()).collect()
}

/// Render rows as cards into `out`. First column = bold heading; the rest =
/// `· Header: value` bullets (header omitted if blank).
fn render_cards(headers: &[String], rows: &[Vec<String>], out: &mut Vec<String>) {
    for (idx, row) in rows.iter().enumerate() {
        if idx > 0 {
            out.push(String::new()); // blank line between cards
        }
        let title = row.first().map(|s| s.as_str()).unwrap_or("");
        out.push(format!("\u{1f538} **{}**", title));
        for (col, cell) in row.iter().enumerate().skip(1) {
            let val = cell.trim();
            if val.is_empty() {
                continue;
            }
            match headers.get(col).map(|h| h.trim()).filter(|h| !h.is_empty()) {
                Some(h) => out.push(format!("\u{00b7} {}: {}", h, val)),
                None => out.push(format!("\u{00b7} {}", val)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_simple_table_to_cards() {
        let input = "\
| 办法 | 一句话 | 累不累 |
|---|---|---|
| 买 | 有公司卖 | 最省 |
| 手动扒 | 浏览器登录跑 | 轻 |";
        let out = tables_to_cards(input);
        assert!(out.contains("\u{1f538} **买**"), "card title: {out}");
        assert!(out.contains("\u{00b7} 一句话: 有公司卖"), "bullet w/ header: {out}");
        assert!(out.contains("\u{00b7} 累不累: 最省"), "{out}");
        assert!(out.contains("\u{1f538} **手动扒**"), "second card: {out}");
        // No raw table pipes left.
        assert!(!out.contains("|---|"), "separator gone: {out}");
        assert!(!out.contains("| 买 |"), "raw row gone: {out}");
    }

    #[test]
    fn leaves_non_table_text_untouched() {
        let input = "这是一段普通文字。\n用了 a | b 这种竖线但不是表格。\n下一行。";
        assert_eq!(tables_to_cards(input), input);
    }

    #[test]
    fn table_with_surrounding_prose() {
        let input = "\
前言一句。

| 列A | 列B |
|---|---|
| x | y |

后记一句。";
        let out = tables_to_cards(input);
        assert!(out.starts_with("前言一句。"), "{out}");
        assert!(out.trim_end().ends_with("后记一句。"), "{out}");
        assert!(out.contains("\u{1f538} **x**"), "{out}");
        assert!(out.contains("\u{00b7} 列B: y"), "{out}");
    }

    #[test]
    fn header_only_table_is_left_alone() {
        // A header + separator with NO body rows isn't rendered as cards.
        let input = "| a | b |\n|---|---|";
        assert_eq!(tables_to_cards(input), input);
    }

    #[test]
    fn cjk_and_wide_table_intact() {
        let input = "\
| 名称 | 描述 | 点赞 | 收藏 | 评论 |
|---|---|---|---|---|
| 笔记一 | 很好的内容 | 100 | 50 | 20 |";
        let out = tables_to_cards(input);
        assert!(out.contains("\u{1f538} **笔记一**"));
        assert!(out.contains("\u{00b7} 描述: 很好的内容"));
        assert!(out.contains("\u{00b7} 点赞: 100"));
        assert!(out.contains("\u{00b7} 评论: 20"));
    }

    #[test]
    fn no_table_is_idempotent() {
        let input = "just a normal reply\nwith two lines";
        assert_eq!(tables_to_cards(input), input);
    }
}
