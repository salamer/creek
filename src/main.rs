mod dag;
use std::collections::HashMap;
fn main() {
    let n = dag::DAGScheduler {};
    n.flow("/Users/chenyangyang/rust/creek/conf.json");
}
