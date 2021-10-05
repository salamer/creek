use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{value::RawValue};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Deserialize)]
struct Node {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    necessary: bool,
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
    starts: Vec<String>,
    nodes: HashMap<String, DAGNode>,
}

fn init(filename: &str) -> Result<(), &'static str>{
    let contents = fs::read_to_string(filename).unwrap();
    let v: DAG = serde_json::from_str(&contents).unwrap();
    let mut dag_config = DAGConfig {
        starts: Vec::new(),
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

    let mut prev_tmp : HashMap<String, HashSet<String>> = HashMap::new();
    let mut next_tmp : HashMap<String, HashSet<String>> = HashMap::new();

    // insert
    for (node_name, node) in &dag_config.nodes {
        for dep in node.node.deps.iter() {
            if dep == &node.node.name {
                return Err("failed");
            }
            if dag_config.nodes.contains_key(&dep.clone()) {
                let mut p = prev_tmp.entry(node.node.name.clone()).or_insert(HashSet::new());
                p.insert(dep.clone());
                let mut n = next_tmp.entry(dep.to_string()).or_insert(HashSet::new());
                n.insert(node.node.name.clone());
            }
            else {
                return Err("failed");
            } 
        }
    }

    return Ok(())
}

fn have_cycle() {}
