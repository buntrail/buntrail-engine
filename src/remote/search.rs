use super::*;
impl SearchService for RemoteServices {
    fn search(
        &self,
        r: SearchRequest,
    ) -> ServiceFuture<'_, Result<EventStream<SearchEvent>, CoreError>> {
        Box::pin(async move {
            self.check(&r.root.target)?;
            Ok(Box::pin(Search {
                source: self.stream(
                    w::Call::Search {
                        request: w::Search::from_core(&r)?,
                    },
                    None,
                )?,
                target: self.target.clone(),
            }) as EventStream<SearchEvent>)
        })
    }
}
struct Search {
    source: Replies,
    target: TargetIdentity,
}
impl Stream for Search {
    type Item = Result<SearchEvent, CoreError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.source).poll_next(cx);
        result.map(|v| {
            v.map(|r| {
                let r = match r {
                    Ok(r) => r,
                    Err(e) => return Ok(SearchEvent::Failed(e)),
                };
                Ok(match r {
                    w::Reply::SearchMatch {
                        file,
                        line,
                        column_utf8,
                        preview,
                        version,
                        digest,
                    } => SearchEvent::Match(Box::new(SearchMatch {
                        file: file.core(&self.target)?,
                        line,
                        preview_column_utf8: column_utf8,
                        preview,
                        searched_version: version.core(&self.target)?,
                        searched_digest: w::digest(&digest)?,
                    })),
                    w::Reply::SearchProgress { files, matches } => {
                        SearchEvent::Progress { files, matches }
                    }
                    w::Reply::SearchComplete => SearchEvent::Complete,
                    w::Reply::SearchCancelled => SearchEvent::Cancelled,
                    w::Reply::SearchLimited { maximum } => SearchEvent::Limited { maximum },
                    _ => return Err(w::invalid()),
                })
            })
        })
    }
}
