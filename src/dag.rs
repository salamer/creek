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
pub enum NodeResult {
    Ok(HashMap<String, Arc<dyn Any + std::marker::Send>>),
    Err(&'static str),
}

unsafe impl Send for NodeResult {}
unsafe impl Sync for NodeResult {}

impl NodeResult {
    pub fn new() -> NodeResult {
        NodeResult::Ok(HashMap::new())
    }
    pub fn safe_get<T: Any + Debug + Clone>(&self, key: &str) -> Option<T> {
        match self {
            NodeResult::Ok(kv) => match kv.get(&key.to_string()) {
                Some(val) => match val.downcast_ref::<T>() {
                    Some(as_t) => return Some(as_t.clone()),
                    None => None,
                },
                None => None,
            },
            NodeResult::Err(_) => None,
        }
    }
    pub fn safe_set<T: Any + Debug + Clone + std::marker::Send>(
        &self,
        key: &str,
        val: &T,
    ) -> NodeResult {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                new_kv.insert(key.to_string(), Arc::new(val.clone()));
                return NodeResult::Ok(new_kv);
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
    fn merge<T: Any + Debug + Clone>(&self, other: &NodeResult) -> NodeResult {
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
    async fn handle<T: DeserializeOwned + Sync, E: Send + Sync>(
        graph_args: &E,
        input: &NodeResult,
        params: &T,
    ) -> NodeResult;
}

#[derive(Deserialize, Default)]
struct AnyParams {}

struct AnyArgs {}

#[derive(Default)]
struct ANode {}

#[async_trait]
impl AsyncNode for ANode {
    async fn handle<T: DeserializeOwned + Sync, E: Send + Sync>(
        graph_args: &E,
        input: &NodeResult,
        params: &T,
    ) -> NodeResult {
        return NodeResult::new();
    }
}

async fn route(node_name: &str) -> impl Sized + AsyncNode {
    match node_name {
        "ANode" => ANode::default(),
        Default => ANode::default(),
    }
}

async fn demo<'a, T, E>(graph_args: T, input: &NodeResult, params: &E) -> NodeResult {
    return NodeResult::new();
}

async fn handle<'a, T: DeserializeOwned>(input: &NodeResult, params: &T) -> NodeResult {
    return NodeResult::new();
}

#[derive(Deserialize, Default)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    necessary: bool,
}

struct DAGNodeWrap<D, F>
where
    D: DeserializeOwned,
    F: futures::Future<Output = NodeResult>,
{
    node_conf: NodeConfig,
    future_handle: Shared<F>,
    params: D,
}

async fn demo1<'a>(params: &NodeResult, n: &'a NodeConfig) {
    let c = handle(params, n);
    let aa = DAGNodeWrap {
        node_conf: NodeConfig::default(),
        future_handle: c.shared(),
        params: NodeConfig::default(),
    };

    aa.future_handle.await;
}

#[derive(Deserialize)]
struct DAG {
    nodes: HashMap<String, NodeConfig>,
}

struct DAGNode {
    node: NodeConfig,
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
