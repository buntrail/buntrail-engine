use buntrail_engine::Engine;
use serde_json::json;

#[test]
fn metadata_exposes_monotonic_language_generation_without_text() {
    let engine = Engine::new();
    let opened = engine.call(json!({"command":"document.open","text":"func f() {}"}));
    let id = opened["value"]["id"].as_u64().unwrap();
    for (generation, language) in [(0, None), (1, Some("typescript".to_owned())), (2, None)] {
        if generation != 0 {
            engine.set_language(id, language).unwrap();
        }
        let status = engine.call(json!({"command":"document.status","id":id}));
        assert_eq!(status["ok"], true);
        assert_eq!(status["value"]["revision"], 0);
        assert_eq!(status["value"]["language_generation"], generation);
        assert!(status["value"].get("text").is_none());
    }
}

#[test]
fn analysis_has_revision_bound_unicode_ranges_and_real_structure() {
    let engine = Engine::new();
    let opened = engine.call(
        json!({"command":"document.open","text":"// 🐇\nfunction hello() {\n return 42;\n}\n"}),
    );
    let id = &opened["value"]["id"];
    let analysis =
        engine.call(json!({"command":"document.analyze","id":id,"language":"javascript"}));
    assert_eq!(analysis["ok"], true, "{analysis}");
    let value = &analysis["value"];
    assert_eq!(value["revision"], 0);
    assert!(
        value["highlights"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["kind"] == "comment" && s["start"] == 0 && s["end"] == 5)
    );
    assert!(
        value["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "hello" && s["start"] == 6)
    );
    assert!(!value["folds"].as_array().unwrap().is_empty());
}

#[test]
fn unsupported_language_is_explicit_and_oversized_analysis_is_bounded() {
    let engine = Engine::new();
    let opened = engine.call(json!({"command":"document.open","text":"text"}));
    let id = &opened["value"]["id"];
    let analysis = engine.call(json!({"command":"document.analyze","id":id,"language":"invented"}));
    assert_eq!(analysis["ok"], false);
    assert_eq!(analysis["error"]["code"], "unsupported_language");
    let big = engine.call(json!({"command":"document.open","text":"x".repeat(1024*1024+1)}));
    assert_eq!(
        engine
            .call(json!({"command":"document.analyze","id":big["value"]["id"],"language":"json"}))
            ["error"]["code"],
        "analysis_limit"
    );
}

#[test]
fn each_base_grammar_accepts_a_real_document() {
    let fixtures = [
        ("js", "function hello() {}"),
        ("ts", "const n: number = 1;"),
        ("tsx", "const x = <h1>Hello</h1>;"),
        ("php", "<?php function hello() {}"),
        ("java", "class Hello {}"),
        ("swift", "struct Hello {}"),
        ("c", "int main(void) {return 0;}"),
        ("cpp", "class Hello {};"),
        ("go", "package main\nfunc main() {}"),
        ("dart", "void main() {}"),
        ("html", "<h1>Hello</h1>"),
        ("xml", "<hello/>"),
        ("md", "# Hello\n"),
        ("yaml", "hello: world\n"),
        ("json", "{\"hello\":1}"),
        ("toml", "hello = 1\n"),
        ("css", "a { color: red; }"),
        ("sh", "echo hello\n"),
    ];
    for (language, text) in fixtures {
        let engine = Engine::new();
        let doc = engine.call(json!({"command":"document.open","text":text}));
        let result = engine.call(
            json!({"command":"document.analyze","id":doc["value"]["id"],"language":language}),
        );
        assert_eq!(result["ok"], true, "{language}: {result}");
        assert_eq!(result["value"]["hasErrors"], false, "{language}: {result}");
    }
}

#[test]
fn editor_outline_and_association_are_additive_to_legacy_analysis() {
    let engine = Engine::new();
    let opened=engine.call(json!({"command":"document.open","text":"function hello() {}","path":"file.js","version":"observed"}));
    let id = opened["value"]["id"].as_u64().unwrap();
    engine.set_language(id, Some("javascript".into())).unwrap();
    let result = engine.call(json!({"command":"document.analyze","id":id,"language":"js"}));
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["value"]["grammar"], "javascript");
    assert!(!result["value"]["outline"].as_array().unwrap().is_empty());
}

#[test]
fn bounded_editor_analysis_keeps_legacy_highlight_cap_and_marks_partial_output() {
    let engine = Engine::new();
    let source = format!("<script>{}</script>", "const value = 1;\n".repeat(8_000));
    let opened = engine.call(json!({"command":"document.open","text":source}));
    let result = engine
        .call(json!({"command":"document.analyze","id":opened["value"]["id"],"language":"html"}));
    if result["ok"] == true {
        assert!(result["value"]["highlights"].as_array().unwrap().len() <= 20_000);
        assert_eq!(result["value"]["truncated"], true);
    } else {
        assert_eq!(result["error"]["code"], "analysis_limit");
    }
}

#[test]
fn editor_analysis_uses_filename_shebang_compound_and_reset_associations() {
    let engine = Engine::new();
    for (path, text, grammar) in [
        ("notes.txt", "plain", "text"),
        ("Dockerfile", "FROM alpine\n", "dockerfile"),
        ("view.blade.php", "<div>{{ $x }}</div>", "blade"),
        ("script", "#!/usr/bin/env node\nconst x=1;", "javascript"),
    ] {
        let opened = engine
            .call(json!({"command":"document.open","text":text,"path":path,"version":"observed"}));
        let id = opened["value"]["id"].as_u64().unwrap();
        let result = engine.call(json!({"command":"document.editor_analyze","id":id}));
        assert_eq!(result["ok"], true, "{path}: {result}");
        assert_eq!(result["value"]["grammar"], grammar);
        engine.set_language(id, Some("text".into())).unwrap();
        assert_eq!(
            engine.call(json!({"command":"document.editor_analyze","id":id}))["value"]["grammar"],
            "text"
        );
        engine.set_language(id, None).unwrap();
        assert_eq!(
            engine.call(json!({"command":"document.editor_analyze","id":id}))["value"]["grammar"],
            grammar
        );
        assert_eq!(
            engine.call(json!({"command":"document.analyze","id":id,"language":"invented"}))["error"]
                ["code"],
            "unsupported_language"
        );
    }
}
