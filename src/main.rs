use futures::future::BoxFuture;
use futures::FutureExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, Semaphore, SemaphorePermit};
use std::time::Instant;

const CONCURRENCY_LIMIT: usize = 1;

#[tokio::main]
async fn main() {
    let (explorer, mut rx) = FSExplorer::new(CONCURRENCY_LIMIT);
    tokio::spawn(walk(explorer, Path::new("/").to_path_buf()));

    let mut counter = 0;
    let mut err_counter = 0;
    let begin = Instant::now();

    while let Some(msg) = rx.next().await {
        match msg {
            Ok(_) => {
                // print!(".");
                counter += 1
            },
            Err(_) => {
                // print!("E");
                err_counter += 1;
            }
        }
    }
    println!("Got {} files and {} errors in {} seconds", counter, err_counter, begin.elapsed().as_secs());
}

async fn walk(explorer: FSExplorer, path: PathBuf) {
    let ticket = explorer.wait_for_turn().await;

    if let Err(e) = inner(explorer.clone(), path.to_path_buf()).await {
        explorer.report_problem(e);
    }

    drop(ticket);
}

fn inner(explorer: FSExplorer, path: PathBuf) -> BoxFuture<'static, anyhow::Result<()>> {
    async move {
        let metadata = tokio::fs::metadata(path.as_path()).await?;
        if metadata.is_dir() {
            let mut entries = tokio::fs::read_dir(&path).await?;
            while let Some(entry) = entries.next().await {
                tokio::spawn(walk(explorer.clone(), entry?.path()));
            }
        } else {
            explorer.report_file(path);
        }
        Ok(())
    }
    .boxed()
}

#[derive(Clone)]
struct FSExplorer {
    sem: Arc<Semaphore>,
    tx: UnboundedSender<anyhow::Result<PathBuf>>,
}

impl FSExplorer {
    pub fn new(concurrency: usize) -> (Self, mpsc::UnboundedReceiver<anyhow::Result<PathBuf>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let new_instance = Self {
            sem: Arc::new(Semaphore::new(concurrency)),
            tx,
        };
        (new_instance, rx)
    }

    pub async fn wait_for_turn(&self) -> SemaphorePermit<'_> {
        self.sem.acquire().await
    }

    pub fn report_file(&self, path: PathBuf) {
        self.tx.send(Ok(path)).expect("tx fail");
    }

    pub fn report_problem(&self, e: anyhow::Error) {
        self.tx.send(Err(e)).expect("tx err fail");
    }
}
