use super::*;
use crate::jobs::Job;
use std::sync::atomic::AtomicBool;
fn event(event: SearchEvent) -> Result<Value> {
    Ok(match event {
        SearchEvent::Match(m) => {
            json!({"kind":"match","path":files_adapter::text_path(&m.file.path)?,"line":m.line,"column":m.preview_column_utf8,"text":m.preview,"searched_version":m.searched_version,"searched_digest":m.searched_digest})
        }
        SearchEvent::Progress { files, matches } => {
            json!({"kind":"progress","files":files,"matches":matches})
        }
        SearchEvent::Complete => json!({"kind":"complete"}),
        SearchEvent::Cancelled => json!({"kind":"cancelled"}),
        SearchEvent::Limited { maximum } => json!({"kind":"limited","maximum":maximum}),
        SearchEvent::Failed(e) => return Err(error(e)),
    })
}
impl Runtime {
    fn search_service(
        &self,
        route: Option<&str>,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<(Arc<dyn SearchService>, TargetIdentity)> {
        if let Some(route) = route {
            let remote = self.route(route)?;
            remote.validate_environment_ready().map_err(error)?;
            let remote = if let Some(cancel) = cancellation {
                Arc::new(remote.with_cancellation(cancel))
            } else {
                remote
            };
            let target = remote.target().clone();
            Ok((remote, target))
        } else {
            let local = self
                .services
                .local_connection()
                .map_err(|e| fail("unavailable", e))?;
            Ok((local.search.clone(), local.identity.clone()))
        }
    }
    fn search_request(r: &Value, target: TargetIdentity) -> Result<SearchRequest> {
        let globs = |key| {
            r.get(key)
                .map(|v| {
                    serde_json::from_value::<Vec<String>>(v.clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))
                })
                .unwrap_or_else(|| Ok(vec![]))
        };
        Ok(SearchRequest {
            request: RequestId(0),
            root: FileRef {
                target,
                path: TargetPath::posix(string(r, "root")?.as_bytes().to_vec()).map_err(error)?,
                resolved_identity: None,
            },
            expression: string(r, "query")?.into(),
            mode: if r["regex"] == true {
                SearchMode::Regex
            } else {
                SearchMode::Literal
            },
            case_sensitive: r["case_sensitive"] != false,
            whole_word: r["whole_word"] == true,
            includes: globs("includes")?,
            excludes: globs("excludes")?,
            hidden: r["hidden"] == true,
            ignored: r["ignored"] == true,
        })
    }
    pub(super) fn search_collect(
        &self,
        route: Option<&str>,
        r: &Value,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Value> {
        let (service, target) = self.search_service(route, cancel)?;
        let mut stream = run(service.search(Self::search_request(r, target)?)).map_err(error)?;
        let mut matches = vec![];
        let mut bytes = 0;
        let mut truncated = false;
        while let Some(item) = run(Box::pin(std::future::poll_fn(|cx| {
            stream.as_mut().poll_next(cx)
        }))) {
            let item = event(item.map_err(error)?)?;
            if item["kind"] == "match" {
                bytes += item["text"].as_str().map_or(0, str::len);
                if matches.len() >= 10000 || bytes > 4 * 1024 * 1024 {
                    truncated = true;
                    break;
                }
                matches.push(item);
            } else if item["kind"] == "limited" {
                truncated = true;
            }
        }
        Ok(json!({"matches":matches,"truncated":truncated}))
    }
    pub fn stream_job(&self, r: &Value, job: &Job) -> Result<bool> {
        let (route, request) = if r["command"] == "remote.call" {
            (Some(string(r, "id")?), &r["request"])
        } else {
            (None, r)
        };
        if request["command"] != "search.stream" {
            return Ok(false);
        }
        let (service, target) = self.search_service(route, Some(job.cancellation()))?;
        let search = Self::search_request(request, target)?;
        let mut stream = job.run(Box::pin(async {
            service.search(search).await.map_err(error)
        }))?;
        while let Some(item) = job.run(Box::pin(async {
            Ok(std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await)
        }))? {
            job.emit(event(item.map_err(error)?)?)?;
        }
        Ok(true)
    }
}
