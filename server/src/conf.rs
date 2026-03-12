use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Serialize, Deserialize, Clone)]
pub struct AofConf {
    pub max_size: usize,
    pub dir: String,
}

#[derive(Serialize, Deserialize)]
pub struct Conf {
    #[serde(default = "default_port")]
    pub port: usize,
    #[serde(default = "default_aof")]
    pub aof: AofConf,
    #[serde(default = "default_lru")]
    pub lru_size: usize,
}

fn default_port() -> usize {
    9090
}

fn default_lru() -> usize {
    1_000_000
}
fn default_aof() -> AofConf {
    AofConf {
        max_size: 1000,
        dir: "aof/log.aof".to_string(),
    }
}
impl Default for Conf {
    fn default() -> Self {
        Self {
            port: default_port(),
            aof: default_aof(),
            lru_size: default_lru(),
        }
    }
}

impl Conf {
    pub async fn load() -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string("conf.json").await?;
        let conf = serde_json::from_str::<Conf>(&conf_str).unwrap_or_default();
        Ok(conf)
    }
}
