use buntrail_engine::Engine;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
fn value(engine: &Engine, request: Value) -> Value {
    let result = engine.call(request);
    assert_eq!(result["ok"], true, "{result}");
    result["value"].clone()
}
fn collect(engine: &Engine, id: &Value) -> Vec<Value> {
    let until = Instant::now() + Duration::from_secs(5);
    let mut events = vec![];
    loop {
        let result = value(engine, json!({"command":"service.poll","id":id}));
        events.extend(result["events"].as_array().unwrap().iter().cloned());
        if result["done"] == true {
            return events;
        }
        assert!(Instant::now() < until, "Job did not finish");
        std::thread::sleep(Duration::from_millis(2));
    }
}
#[test]
fn completed_job_delivers_exactly_one_terminal_event_and_released_ids_stay_stale() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new();
    let id = value(
        &engine,
        json!({"command":"service.start","request":{"command":"fs.list","path":root.path()}}),
    )["id"]
        .clone();
    let events = collect(&engine, &id);
    assert_eq!(events.iter().filter(|e| e["kind"] == "item").count(), 1);
    assert_eq!(events.iter().filter(|e| e["kind"] == "finished").count(), 1);
    assert!(
        value(&engine, json!({"command":"service.poll","id":id}))["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    value(&engine, json!({"command":"service.cancel","id":id}));
    assert!(
        value(&engine, json!({"command":"service.poll","id":id}))["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    value(&engine, json!({"command":"service.release","id":id}));
    assert_eq!(
        engine.call(json!({"command":"service.poll","id":id}))["ok"],
        false
    );
    let next = value(
        &engine,
        json!({"command":"service.start","request":{"command":"fs.list","path":root.path()}}),
    )["id"]
        .clone();
    assert_ne!(id, next);
}
#[test]
fn retained_capacity_is_bounded_and_engine_commands_are_not_admitted() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new();
    let mut ids = vec![];
    for _ in 0..32 {
        ids.push(value(&engine,json!({"command":"service.start","request":{"command":"fs.list","path":root.path()}}))["id"].clone());
    }
    assert_eq!(
        engine.call(
            json!({"command":"service.start","request":{"command":"fs.list","path":root.path()}})
        )["error"]["code"],
        "busy"
    );
    for id in ids {
        value(&engine, json!({"command":"service.cancel","id":id}));
        value(&engine, json!({"command":"service.release","id":id}));
    }
    assert_eq!(engine.call(json!({"command":"service.start","request":{"command":"document.close","id":1,"discard":true}}))["error"]["code"],"invalid_request");
}
#[test]
fn oversized_results_fail_once_instead_of_growing_the_queue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("large");
    std::fs::write(&path, vec![b'x'; 80 * 1024]).unwrap();
    let engine = Engine::new();
    let id = value(
        &engine,
        json!({"command":"service.start","request":{"command":"fs.read","path":path}}),
    )["id"]
        .clone();
    let events = collect(&engine, &id);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["kind"], "failed");
    assert!(serde_json::to_vec(&events).unwrap().len() < 64 * 1024);
}
