use hyper::Request;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Semaphore;
use tower::Service;

/// Tower layer that limits concurrent HTTP requests (in-flight limit)
#[derive(Clone)]
pub struct RequestLimitLayer {
    semaphore: Arc<Semaphore>,
}

impl RequestLimitLayer {
    pub fn new(max_requests: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_requests)),
        }
    }
}

impl<S> tower::Layer<S> for RequestLimitLayer {
    type Service = RequestLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestLimitService {
            inner,
            semaphore: Arc::clone(&self.semaphore),
        }
    }
}

/// Service that enforces request limits
#[derive(Clone)]
pub struct RequestLimitService<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
}

impl<S, ReqBody> Service<Request<ReqBody>> for RequestLimitService<S>
where
    S: Service<Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let semaphore = Arc::clone(&self.semaphore);

        Box::pin(async move {
            // Acquire permit - blocks if limit reached
            let _permit = semaphore.acquire().await.expect("semaphore closed");

            // Process request while holding permit
            inner.call(req).await
        })
    }
}
