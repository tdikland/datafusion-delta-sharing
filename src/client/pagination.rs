use std::{
    future::Future,
    marker::PhantomData,
    pin::{pin, Pin},
    task::{ready, Context, Poll},
};

use futures::Stream;
use pin_project_lite::pin_project;

#[derive(Clone)]
pub struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
    done: bool,
}

impl Pagination {
    pub fn start() -> Self {
        Self {
            max_results: Some(2),
            page_token: None,
            done: false,
        }
    }

    pub fn with_max_results(&mut self, max_results: Option<u32>) -> &mut Self {
        self.max_results = max_results;
        self
    }

    pub fn max_results(&self) -> Option<i32> {
        self.max_results.map(|mr| mr.try_into().expect("valid"))
    }

    pub fn page_token(&self) -> Option<String> {
        self.page_token.clone()
    }

    pub fn advance(&mut self, next_page_token: Option<String>) {
        self.page_token = next_page_token;
        self.done = self.page_token.is_none();
    }

    pub fn is_done(&self) -> bool {
        self.done
    }
}

pub struct Page<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }
}

pin_project! {
    #[project = StateProj]
    enum State<T, E, Fut> {
        Start,
        Fetch {
            #[pin]
            fut: Fut
        },
        YieldAndFetch {
            items: Vec<T>,
            #[pin]
            fut: Fut
        },
        YieldNextReady {
            items: Vec<T>,
            next: Vec<T>,
        },
        Yield {
            items: Vec<T>
        },
        Error {
            err: Option<E>
        },
        End
    }

}

pin_project! {
    pub struct Paginated<T, E, F, Fut> {
        #[pin]
        state: State<T, E, Fut>,
        fetch_page: F,
        pagination: Pagination,
    }
}

impl<T, E, F, Fut> Paginated<T, E, F, Fut>
where
    F: Fn(Pagination) -> Fut,
    Fut: Future<Output = Result<Page<T>, E>>,
{
    pub fn new(fetch: F) -> Self {
        Self {
            fetch_page: fetch,
            state: State::Start,
            pagination: Pagination::start(),
        }
    }
}

impl<T, E, F, Fut> Stream for Paginated<T, E, F, Fut>
where
    F: Fn(Pagination) -> Fut,
    Fut: Future<Output = Result<Page<T>, E>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Start => {
                    let fut = (this.fetch_page)(this.pagination.clone());
                    this.state.set(State::Fetch { fut });
                }
                StateProj::Fetch { fut } => {
                    let page = ready!(fut.poll(cx));
                    let page = if let Ok(page) = page {
                        page
                    } else {
                        this.state.set(State::End);
                        return Poll::Ready(None);
                    };
                    if let Some(next_token) = page.next_page_token {
                        this.pagination.page_token = Some(next_token);
                        this.state.set(State::YieldAndFetch {
                            items: page.items,
                            fut: (this.fetch_page)(this.pagination.clone()),
                        });
                    } else {
                        this.state.set(State::Yield { items: page.items });
                    }
                }
                StateProj::YieldAndFetch { items, fut } if !items.is_empty() => {
                    let page = ready!(fut.poll(cx));
                    let page = match page {
                        Ok(page) => page,
                        Err(err) => {
                            this.state.set(State::Error { err: Some(err) });
                            continue;
                        }
                    };
                    let yield_item = items.pop().expect("not empty");
                    let items = std::mem::take(items);
                    this.pagination.advance(page.next_page_token);
                    this.state.set(State::YieldNextReady {
                        items,
                        next: page.items,
                    });
                    return Poll::Ready(Some(Ok(yield_item)));
                }
                StateProj::YieldAndFetch { items, fut } if items.is_empty() => {
                    let page = ready!(fut.poll(cx));
                    let page = match page {
                        Ok(page) => page,
                        Err(err) => {
                            this.state.set(State::Error { err: Some(err) });
                            continue;
                        }
                    };
                    this.pagination.advance(page.next_page_token);
                    if this.pagination.is_done() {
                        this.state.set(State::Yield { items: page.items });
                    } else {
                        this.state.set(State::YieldAndFetch {
                            items: page.items,
                            fut: (this.fetch_page)(this.pagination.clone()),
                        });
                    }
                }
                StateProj::YieldAndFetch { .. } => unreachable!(),
                StateProj::YieldNextReady { items, next } => {
                    if let Some(item) = items.pop() {
                        return Poll::Ready(Some(Ok(item)));
                    } else {
                        let next = std::mem::take(next);
                        if this.pagination.is_done() {
                            this.state.set(State::Yield { items: next });
                        } else {
                            this.state.set(State::YieldAndFetch {
                                items: next,
                                fut: (this.fetch_page)(this.pagination.clone()),
                            });
                        }
                    }
                }
                StateProj::Yield { items } => {
                    if items.is_empty() {
                        this.state.set(State::End);
                    } else {
                        return Poll::Ready(Some(Ok(items.remove(0))));
                    }
                }
                StateProj::Error { err } => {
                    let err = std::mem::take(err).expect("err");
                    this.state.set(State::End);
                    return Poll::Ready(Some(Err(err)));
                }
                StateProj::End => return Poll::Ready(None),
            }
        }
    }
}

pin_project! {
    pub struct PP<R, S> {
        #[pin]
        inner: S,
        _p: PhantomData<R>
    }
}

impl<T, E, R, S> Stream for PP<R, S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}
