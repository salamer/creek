mod dag;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub enum NodeResult<'a> {
    Value(HashMap<&'a str, Arc<dyn Any>>),
    Err(&'static str),
}

impl<'a> NodeResult<'a> {
    pub fn new() -> NodeResult<'a> {
        NodeResult::Value(HashMap::new())
    }
    pub fn safe_get<T: Any + Debug + Clone>(&self, key: &str) -> Option<T> {
        match self {
            NodeResult::Value(kv) => match kv.get(&key) {
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
            NodeResult::Value(kv) => {
                let mut new_kv = kv.clone();
                new_kv.insert(key, Arc::new(val.clone()));
                return NodeResult::Value(new_kv);
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
    fn merge<T: Any + Debug + Clone>(&self, other: &'a NodeResult) -> NodeResult<'_> {
        match self {
            NodeResult::Value(kv) => {
                let mut new_kv = kv.clone();
                match other {
                    NodeResult::Value(other_kv) => new_kv.extend(other_kv.clone()),
                    NodeResult::Err(e) => {}
                }
                return NodeResult::Value(new_kv);
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
