use async_trait::async_trait;
use futures::future::join_all;
use futures::future::FutureExt;
use futures::future::Shared;

use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::pin::Pin;
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
                Some(val) => val.downcast_ref::<T>().cloned(),
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
                NodeResult::Ok(new_kv)
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
                    NodeResult::Err(_e) => {}
                }
                NodeResult::Ok(new_kv)
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
    ) -> NodeResult;
}

#[derive(Deserialize, Default, Copy, Clone)]
struct AnyParams {
    val: i32,
}

struct AnyArgs {}

#[derive(Default)]
struct ANode {}

impl ANode {
    fn to_params(_input: &str) -> AnyParams {
        AnyParams::default()
    }
}

#[async_trait]
impl AsyncNode for ANode {
    type Params = AnyParams;
    async fn handle<E: Send + Sync>(
        _graph_args: Arc<E>,
        _input: Arc<NodeResult>,
        _params: Arc<AnyParams>,
    ) -> NodeResult {
        return NodeResult::new();
    }
}

async fn route(node_name: &str) -> impl Sized + AsyncNode {
    match node_name {
        "ANode" => ANode::default(),
        _Default => ANode::default(),
    }
}

async fn handle() -> NodeResult {
    NodeResult::new()
}

#[derive(Deserialize, Default, Debug, Clone)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    necessary: bool,
}

struct DAGNode<F>
where
    F: futures::Future<Output = NodeResult>,
{
    node_conf: NodeConfig,
    future_handle: Shared<F>,
    params: Box<RawValue>,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
}

struct DAGNodeConfig {
    node_conf: NodeConfig,
    params: Box<RawValue>,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
}

#[derive(Deserialize)]
struct DAG {
    nodes: Vec<NodeConfig>,
}

struct DAGConfig {
    nodes: HashMap<String, DAGNodeConfig>,
}

pub struct DAGScheduler {
    params_map: HashMap<String, Box<RawValue>>,
}

impl DAGScheduler {
    pub fn new() -> DAGScheduler {
        DAGScheduler {
            params_map: HashMap::new(),
        }
    }

    pub fn init(&mut self, filename: &str) -> Result<(), &'static str> {
        let contents = fs::read_to_string(filename).unwrap();
        println!("content {:?}", contents);
        let v: DAG = serde_json::from_str(&contents).unwrap();
        let mut dag_config = DAGConfig {
            nodes: HashMap::new(),
        };

        for node in v.nodes.into_iter() {
            println!("node {:?}", node);
            dag_config.nodes.insert(
                node.name.clone(),
                DAGNodeConfig {
                    node_conf: node.clone(),
                    nexts: HashSet::new(),
                    prevs: HashSet::new(),
                    params: node.params.clone(),
                },
            );
        }

        let mut prev_tmp: HashMap<String, HashSet<String>> = HashMap::new();
        let mut next_tmp: HashMap<String, HashSet<String>> = HashMap::new();

        // insert
        for node in dag_config.nodes.values() {
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
        println!("papapa \n {:?} {:?}", prev_tmp, next_tmp);

        // TODO: has cycle?
        // TODO: has only one leaf node

        let mut entry_nodes = HashSet::new();
        let mut leaf_nodes = HashSet::new();

        for (node_name, node) in &dag_config.nodes {
            if node.prevs.is_empty() {
                entry_nodes.insert(node_name.clone());
            }

            if node.nexts.is_empty() {
                leaf_nodes.insert(node_name.clone());
            }
        }

        let mut dag_center = HashMap::new();
        let mut DAGNames = Vec::new();
        let mut prev_map = HashMap::new();
        let mut params_map = HashMap::new();
        let args: Arc<i32> = Arc::new(1);
        let _params = Arc::new(AnyParams::default());

        for (node_name, node) in dag_config.nodes {
            let entry = async { NodeResult::new() };
            dag_center.insert(
                node_name.clone(),
                Box::new(DAGNode {
                    node_conf: node.node_conf,
                    nexts: node.nexts.clone(),
                    prevs: node.prevs.clone(),
                    params: node.params.clone(),
                    future_handle: entry.boxed().shared(),
                }),
            );
            DAGNames.push(node_name.clone());
            prev_map.insert(node_name.clone(), node.prevs.clone());
            params_map.insert(node_name.clone(), node.params.clone());
        }

        for node_name in DAGNames.iter() {
            let mut deps = Vec::new();
            for dep in prev_map.get(&node_name.clone()).unwrap().iter() {
                deps.push(dag_center.get(dep).unwrap().future_handle.clone());
            }

            let nn = node_name.clone();
            let mut node = dag_center.get_mut(node_name).unwrap();
            let raw = Arc::new("");
            let any_params = Arc::new(node.params.get());
            // let c = async { Arc::clone(&any_params) };
            let a = Arc::clone(&args);
            let my_params_map = self.params_map.get(node_name).unwrap().clone();
            node.future_handle = join_all(deps)
                .then(|x| async move {
                    let params: AnyParams = serde_json::from_str(my_params_map.get()).unwrap();
                    ANode::handle::<i32>(
                        a,
                        Arc::new(x.iter().fold(NodeResult::new(), |a, b| a.merge(b))),
                        Arc::new(AnyParams::default()),
                    )
                    .await
                })
                .boxed()
                .shared();
        }

        let r = async {
            let leaf_nodes: Vec<_> = leaf_nodes
                .iter()
                .map(|x| dag_center.get(x).unwrap().future_handle.clone())
                .collect();
            let x = join_all(leaf_nodes).await;
            x[0].clone()
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(r);

        Ok(())
    }

    pub fn flow(&self, filename: &str) {}
}

fn have_cycle(_x: i32) {}
