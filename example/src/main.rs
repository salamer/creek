use futures::future;
use futures::prelude::*;
use tokio::task;

async fn app(i: i32) {
    println!("hello {}\n", i);
}

async fn demo() {
    let future1 = app(1).shared();
    let future2 = app(2).shared();
    let future3 = app(3).shared();

    let a = async {
        future::join_all(vec![future1.clone(), future2.clone()]).await;
    };

    let b = async {
        future::join_all(vec![future2.clone(), future3.clone()]).await;
    };

    a.await;
    b.await;
}

fn main() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(demo());
}
