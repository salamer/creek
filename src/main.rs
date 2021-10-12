mod dag;

fn main() {
    let mut n = dag::DAGScheduler::new();
    n.init("/Users/chenyangyang/rust/creek/conf.json");
}
