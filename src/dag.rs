use async_trait::async_trait;
use futures::future;
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
    fn merge(&self, other: &NodeResult) -> NodeResult {
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
    type Params;
    async fn handle<E: Send + Sync>(
        graph_args: Arc<E>,
        input: Arc<NodeResult>,
        params: Arc<Self::Params>,
    ) -> Arc<NodeResult>;
}

#[derive(Deserialize, Default, Copy, Clone)]
struct AnyParams {}

struct AnyArgs {}

#[derive(Default)]
struct ANode {}

impl ANode {
    fn to_params(input: &str) -> AnyParams {
        AnyParams::default()
    }
}

#[async_trait]
impl AsyncNode for ANode {
    type Params = AnyParams;
    async fn handle<E: Send + Sync>(
        graph_args: Arc<E>,
        input: Arc<NodeResult>,
        params: Arc<AnyParams>,
    ) -> Arc<NodeResult> {
        return Arc::new(NodeResult::new());
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

async fn handle() -> Arc<NodeResult> {
    Arc::new(NodeResult::new())
}

#[derive(Deserialize, Default)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    necessary: bool,
}

struct DAGNode<F>
where
    F: futures::Future<Output = Arc<NodeResult>>,
{
    node_conf: NodeConfig,
    future_handle: Shared<F>,
    // params: Option<D>,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
}

struct DAGNodeConfig {
    node_conf: NodeConfig,
    // params: Option<D>,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
}

#[derive(Deserialize)]
struct DAG {
    nodes: HashMap<String, NodeConfig>,
}

struct DAGConfig {
    nodes: HashMap<String, DAGNodeConfig>,
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
            DAGNodeConfig {
                node_conf: NodeConfig::default(),
                nexts: HashSet::new(),
                prevs: HashSet::new(),
                // params: None,
            },
        );
    }

    let mut prev_tmp: HashMap<String, HashSet<String>> = HashMap::new();
    let mut next_tmp: HashMap<String, HashSet<String>> = HashMap::new();

    // insert
    for (node_name, node) in &dag_config.nodes {
        for dep in node.node_conf.deps.iter() {
            if dep == &node.node_conf.name {
                return Err("failed");
            }
            if dag_config.nodes.contains_key(&dep.clone()) {
                prev_tmp
                    .entry(node.node_conf.name.clone())
                    .or_insert(HashSet::new())
                    .insert(dep.clone());
                next_tmp
                    .entry(dep.to_string())
                    .or_insert(HashSet::new())
                    .insert(node.node_conf.name.clone());
            } else {
                return Err("failed");
            }
        }
    }

    // TODO: has cycle?
    // TODO: has only one leaf node

    let mut entry_nodes = HashSet::new();
    let mut leaf_nodes = HashSet::new();

    for (node_name, node) in &dag_config.nodes {
        if node.prevs.len() == 0 {
            entry_nodes.insert(node_name.clone());
        }

        if node.nexts.len() == 0 {
            leaf_nodes.insert(node_name.clone());
        }
    }

    let mut DAGManager = HashMap::new();
    let mut DAGNames = Vec::new();
    let mut prev_map = HashMap::new();

    for (node_name, node) in dag_config.nodes {
        DAGManager.insert(
            node_name.clone(),
            Box::new(DAGNode {
                node_conf: node.node_conf,
                nexts: node.nexts.clone(),
                prevs: node.prevs.clone(),
                // params: None,
                future_handle: handle().shared(),
            }),
        );
        DAGNames.push(node_name.clone());
        prev_map.insert(node_name.clone(), node.prevs.clone());
    }

    let args: Arc<i32> = Arc::new(1);
    let params = Arc::new(AnyParams::default());

    for node_name in DAGNames.iter() {
        let mut deps = Vec::new();

        for dep in prev_map.get(&node_name.clone()).unwrap().iter() {
            deps.push(DAGManager.get(dep).unwrap().future_handle.clone());
        }

        let mut node = DAGManager.get_mut(node_name).unwrap();
        //  handle().shared();

        let p = async {
            let n = future::join_all(deps)
                .then(|x| async move {
                    // ANode::handle::<i32>(
                    //     args,
                    //     Arc::new(x.iter().fold(NodeResult::new(), |a, b| a.merge(b))),
                    //     params,
                    // )
                    handle()
                })
                .await;
            n 
        };
        node.future_handle = p.shared();
    }

    return Ok(());
}

// impl DAGScheduler {
//     fn run()
// }

fn have_cycle(x: i32) {}
