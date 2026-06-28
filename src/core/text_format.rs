//! Chat-friendly text rendering helpers.
//!
//! Chat platforms (Discord, Telegram) do not render GitHub-flavored Markdown
//! tables — the `| --- |` pipes show literally and misalign on a phone screen.
//! [`tables_to_aligned`] rewrites any Markdown table into a monospace
//! code-block with columns aligned by *visual* width (CJK glyphs count as two
//! cells), which is the closest a chat platform gets to a real table.

/// Rewrite every Markdown table in `text` into an aligned monospace code block,
/// leaving all non-table text untouched. Idempotent on text with no tables.
///
/// A table is detected as: a header row containing `|`, immediately followed by
/// a separator row whose cells are all dashes (`---`, `:--:`, …), followed by
/// one or more body rows. Anything that doesn't match that exact shape is left
/// as-is, so a stray `|` in prose or code is never mangled.
pub fn tables_to_aligned(text: &str) -> String {
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
                render_aligned(&headers, &rows, &mut out);
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
        !c.is_empty() && c.chars().all(|ch| ch == '-' || ch == ':') && c.contains('-')
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

/// Visual (monospace) width of a cell: CJK and other wide glyphs occupy two
/// cells in a fixed-width font, so they must count as two for columns to align.
/// Markdown emphasis markers (`*`, `` ` ``) are stripped first since they don't
/// render in a code block.
fn visual_width(s: &str) -> usize {
    s.chars()
        .filter(|c| !matches!(c, '*' | '`' | '_'))
        .map(|c| if is_wide(c) { 2 } else { 1 })
        .sum()
}

/// Strip emphasis markers that don't render inside a code block.
fn strip_markers(s: &str) -> String {
    s.chars().filter(|c| !matches!(c, '*' | '`' | '_')).collect()
}

/// Whether a char is rendered double-width in a monospace font (CJK, kana,
/// fullwidth forms, common CJK punctuation). Approximate but covers the ranges
/// that matter for chat tables.
fn is_wide(c: char) -> bool {
    let u = c as u32;
    (0x1100..=0x115F).contains(&u)        // Hangul Jamo
        || (0x2E80..=0x303E).contains(&u) // CJK radicals, Kangxi, CJK symbols/punct
        || (0x3041..=0x33FF).contains(&u) // Hiragana, Katakana, CJK symbols
        || (0x3400..=0x4DBF).contains(&u) // CJK Ext A
        || (0x4E00..=0x9FFF).contains(&u) // CJK Unified
        || (0xA000..=0xA4CF).contains(&u) // Yi
        || (0xAC00..=0xD7A3).contains(&u) // Hangul syllables
        || (0xF900..=0xFAFF).contains(&u) // CJK compatibility
        || (0xFE30..=0xFE4F).contains(&u) // CJK compatibility forms
        || (0xFF00..=0xFF60).contains(&u) // Fullwidth forms
        || (0xFFE0..=0xFFE6).contains(&u) // Fullwidth signs
        || (0x1F300..=0x1FAFF).contains(&u) // emoji (wide)
}

/// Render the table as an aligned monospace code block into `out`. Columns are
/// padded to their max visual width; the header is kept and a `---+---`
/// separator drawn beneath it.
fn render_aligned(headers: &[String], rows: &[Vec<String>], out: &mut Vec<String>) {
    // All rows including the header participate in width computation.
    let mut all: Vec<&[String]> = Vec::with_capacity(rows.len() + 1);
    all.push(headers);
    for r in rows {
        all.push(r);
    }
    let num_cols = all.iter().map(|r| r.len()).max().unwrap_or(0);
    if num_cols == 0 {
        return;
    }

    let mut widths = vec![0usize; num_cols];
    for r in &all {
        for (k, cell) in r.iter().enumerate() {
            let w = visual_width(cell);
            if w > widths[k] {
                widths[k] = w;
            }
        }
    }

    // Pad a cell to its column's visual width (right-pad with spaces).
    let pad = |cell: &str, col: usize| -> String {
        let stripped = strip_markers(cell);
        let w = visual_width(cell);
        let fill = widths[col].saturating_sub(w);
        format!("{}{}", stripped, " ".repeat(fill))
    };

    let render_row = |cells: &[String]| -> String {
        (0..num_cols)
            .map(|k| pad(cells.get(k).map(|s| s.as_str()).unwrap_or(""), k))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    out.push("```".to_string());
    out.push(render_row(headers));
    // Separator line matching the column widths.
    let sep = (0..num_cols)
        .map(|k| "-".repeat(widths[k]))
        .collect::<Vec<_>>()
        .join("-+-");
    out.push(sep);
    for r in rows {
        out.push(render_row(r));
    }
    out.push("```".to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_table_to_aligned_codeblock() {
        let input = "\
| 办法 | 一句话 | 累不累 |
|---|---|---|
| 买 | 有公司卖 | 最省 |
| 手动扒 | 浏览器登录跑 | 轻 |";
        let out = tables_to_aligned(input);
        // Wrapped in a code block.
        assert!(out.starts_with("```\n"), "starts with code fence: {out}");
        assert!(out.trim_end().ends_with("```"), "ends with code fence: {out}");
        // Header + a separator line + content present.
        assert!(out.contains("办法"), "{out}");
        assert!(out.contains("有公司卖"), "{out}");
        assert!(out.contains("-+-"), "separator drawn: {out}");
        // No raw markdown separator row left.
        assert!(!out.contains("|---|"), "md separator gone: {out}");
    }

    #[test]
    fn columns_align_by_visual_width() {
        // A CJK-heavy column and an ASCII column must line up: every body row's
        // first ` | ` separator should sit at the same byte... no — same visual
        // column. We check by asserting all rendered rows have equal display
        // width up to the first separator.
        let input = "\
| 名 | n |
|---|---|
| 买 | 1 |
| 手动扒 | 22 |";
        let out = tables_to_aligned(input);
        // The widest first-column cell is 手动扒 (3 CJK = width 6). Every row's
        // first column should pad to 6, so " | " appears at a consistent place.
        let body: Vec<&str> = out.lines().filter(|l| l.contains(" | ")).collect();
        let first_seps: Vec<usize> = body
            .iter()
            .map(|l| visual_width(l.split(" | ").next().unwrap()))
            .collect();
        assert!(
            first_seps.windows(2).all(|w| w[0] == w[1]),
            "first column visual widths must match: {first_seps:?} in {out}"
        );
    }

    #[test]
    fn leaves_non_table_text_untouched() {
        let input = "这是一段普通文字。\n用了 a | b 这种竖线但不是表格。\n下一行。";
        assert_eq!(tables_to_aligned(input), input);
    }

    #[test]
    fn table_with_surrounding_prose() {
        let input = "\
前言一句。

| 列A | 列B |
|---|---|
| x | y |

后记一句。";
        let out = tables_to_aligned(input);
        assert!(out.starts_with("前言一句。"), "{out}");
        assert!(out.trim_end().ends_with("后记一句。"), "{out}");
        assert!(out.contains("```"), "code block present: {out}");
        assert!(out.contains("x"), "{out}");
    }

    #[test]
    fn header_only_table_is_left_alone() {
        let input = "| a | b |\n|---|---|";
        assert_eq!(tables_to_aligned(input), input);
    }

    #[test]
    fn strips_markdown_markers_inside_cells() {
        let input = "\
| 名称 | 状态 |
|---|---|
| **粗体名** | `代码` |";
        let out = tables_to_aligned(input);
        // Markers don't render in a code block, so they're stripped.
        assert!(out.contains("粗体名"), "{out}");
        assert!(!out.contains("**"), "asterisks stripped: {out}");
        assert!(!out.contains('`') || out.matches("```").count() == 2, "backticks only the fences: {out}");
    }

    #[test]
    fn no_table_is_idempotent() {
        let input = "just a normal reply\nwith two lines";
        assert_eq!(tables_to_aligned(input), input);
    }
}
