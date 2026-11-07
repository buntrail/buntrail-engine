//! Legacy response adapter over the imported cached language implementation.
use super::{Result, fail};
use buntrail_core::{Snapshot, contracts::*};
use buntrail_language::LanguageAdapter;
use serde_json::{Value, json};

pub(super) fn canonical(name: &str) -> Result<&'static str> {
    let name = match name {
        "mjs" | "cjs" => "javascript",
        "mts" | "cts" => "typescript",
        "h" => "c",
        "cc" | "hpp" | "hxx" => "cpp",
        "htm" => "html",
        "svg" => "xml",
        other => other,
    };
    buntrail_language::registry::alias(name).ok_or_else(|| {
        fail(
            "unsupported_language",
            format!(
                "Structural grammar unavailable for {name}; plain text editing remains available"
            ),
        )
    })
}
fn outline(node: &OutlineNode) -> Value {
    json!({"id":node.id,"parentID":node.parent_id,"kind":node.kind,"label":node.label,"preview":node.preview,"selection":{"start":node.selection.start,"end":node.selection.end},"enclosing":{"start":node.range.start,"end":node.range.end},"children":node.children.iter().map(outline).collect::<Vec<_>>()})
}
fn symbols(nodes: &[OutlineNode], out: &mut Vec<Value>) {
    for node in nodes {
        out.push(json!({"name":node.label,"start":node.range.start,"end":node.range.end,"kind":node.kind}));
        symbols(&node.children, out);
    }
}
pub(super) fn analyze(
    adapter: &LanguageAdapter,
    id: u64,
    snapshot: Snapshot,
    name: &str,
) -> Result<Value> {
    let canonical = canonical(name)?;
    if snapshot.text.len() > 1024 * 1024 {
        return Err(fail(
            "analysis_limit",
            "Structural analysis is disabled above 1 MiB; text remains editable",
        ));
    }
    let mut document = [0; 16];
    document[..8].copy_from_slice(&id.to_le_bytes());
    let input = DocumentSnapshot {
        document: DocumentID(document),
        revision: BufferRevision(snapshot.revision),
        text: snapshot.text.clone(),
        encoding: Encoding::Utf8 { bom: false },
        line_endings: LineEndings::Lf,
        language: Some(canonical.into()),
    };
    let analyzed =
        adapter.analyze_snapshot_with_limits(input, std::time::Duration::from_millis(300), 20_000);
    if let LanguageAvailability::Unavailable(error) = &analyzed.availability {
        return Err(fail("analysis_limit", error.message.clone()));
    }
    let mut names = vec![];
    symbols(&analyzed.outline, &mut names);
    let mut starts = vec![0];
    for (offset, unit) in snapshot.text.as_str().encode_utf16().enumerate() {
        if unit == 10 {
            starts.push(offset as u64 + 1);
        }
    }
    let line = |offset: u64| {
        starts
            .partition_point(|start| *start <= offset)
            .saturating_sub(1)
    };
    let highlights = analyzed
        .highlights
        .iter()
        .take(20_000)
        .map(|h| json!({"start":h.range.start,"end":h.range.end,"kind":h.scope}))
        .collect::<Vec<_>>();
    let folds=analyzed.folds.iter().map(|f|json!({"start":f.range.start,"end":f.range.end,"line":line(f.range.start),"endLine":line(f.range.end),"id":f.id})).collect::<Vec<_>>();
    Ok(
        json!({"id":id,"revision":snapshot.revision,"language":name,"highlights":highlights,"folds":folds,"symbols":names,"hasErrors":analyzed.has_errors,"truncated":analyzed.truncated || analyzed.highlights.len()>20_000,"grammar":analyzed.grammar_id,"queryVersion":analyzed.query_version,"available":true,"outline":analyzed.outline.iter().map(outline).collect::<Vec<_>>(),"injections":analyzed.injections.iter().map(|i|json!({"start":i.range.start,"end":i.range.end,"language":i.language})).collect::<Vec<_>>()}),
    )
}
