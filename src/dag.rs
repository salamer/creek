use async_trait::async_trait;
use futures::future::Shared;
use futures::prelude::Future;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub enum NodeResult<'a> {
    Ok(HashMap<&'a str, Arc<dyn Any>>),
    Err(&'static str),
}

impl<'a> NodeResult<'a> {
    pub fn new() -> NodeResult<'a> {
        NodeResult::Ok(HashMap::new())
    }
    pub fn safe_get<T: Any + Debug + Clone>(&self, key: &str) -> Option<T> {
        match self {
            NodeResult::Ok(kv) => match kv.get(&key) {
                Some(val) => match val.downcast_ref::<T>() {
                    Some(as_t) => return Some(as_t.clone()),
                    None => None,
                },
                None => None,
            },
            NodeResult::Err(_) => None,
        }
    }
    pub fn safe_set<T: Any + Debug + Clone>(&self, key: &'a str, val: &T) -> NodeResult<'_> {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                new_kv.insert(key, Arc::new(val.clone()));
                return NodeResult::Ok(new_kv);
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
    fn merge<T: Any + Debug + Clone>(&self, other: &'a NodeResult) -> NodeResult<'_> {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                match other {
                    NodeResult::Ok(other_kv) => new_kv.extend(other_kv.clone()),
                    NodeResult::Err(e) => {}
                }
                return NodeResult::Ok(new_kv);
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
}

#[async_trait]
pub trait AsyncNode {
    type Params: DeserializeOwned;
    async fn handle<'a, T: DeserializeOwned>(
        input: &'a NodeResult,
        params: &Self::Params,
    ) -> NodeResult<'a>;
}

async fn demo<'a, T: DeserializeOwned>(input: &NodeResult<'_>) -> NodeResult<'a> {
    return NodeResult::new();
}

async fn handle<'a, T: DeserializeOwned>(input: &NodeResult<'a>, params: &T) -> NodeResult<'a> {
    return NodeResult::new();
}

#[derive(Deserialize, Default)]
struct Node {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    necessary: bool,
}

struct DAGNodeWrap<'b, D, F>
where
    D: DeserializeOwned,
    F: futures::Future<Output = NodeResult<'b>>,
{
    node: Node,
    f: Shared<F>,
    params: D,
}

async fn demo1<'a>(params: &NodeResult<'a>, n: &'a Node) {
    let c = handle(params, n);
    let aa = DAGNodeWrap {
        node: Node::default(),
        f: c.shared(),
        params: Node::default(),
    };

    aa.f.await;
}

#[derive(Deserialize)]
struct DAG {
    nodes: HashMap<String, Node>,
}

struct DAGNode {
    node: Node,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
}

struct DAGConfig {
    nodes: HashMap<String, DAGNode>,
}

struct DAGScheduler {}

fn init(filename: &str) -> Result<(), &'static str> {
    let contents = fs::read_to_string(filename).unwrap();
    let v: DAG = serde_json::from_str(&contents).unwrap();
    let mut dag_config = DAGConfig {
        nodes: HashMap::new(),
    };

    for (key, node) in v.nodes.into_iter() {
        dag_config.nodes.insert(
            node.name.clone(),
            DAGNode {
                node: node,
                nexts: HashSet::new(),
                prevs: HashSet::new(),
            },
        );
    }

    let mut prev_tmp: HashMap<String, HashSet<String>> = HashMap::new();
    let mut next_tmp: HashMap<String, HashSet<String>> = HashMap::new();

    // insert
    for (node_name, node) in &dag_config.nodes {
        for dep in node.node.deps.iter() {
            if dep == &node.node.name {
                return Err("failed");
            }
            if dag_config.nodes.contains_key(&dep.clone()) {
                let mut p = prev_tmp
                    .entry(node.node.name.clone())
                    .or_insert(HashSet::new());
                p.insert(dep.clone());
                let mut n = next_tmp.entry(dep.to_string()).or_insert(HashSet::new());
                n.insert(node.node.name.clone());
            } else {
                return Err("failed");
            }
        }
    }

    return Ok(());
}

// impl DAGScheduler {
//     fn run()
// }

fn have_cycle() {}
