use crate::rpc::RpcClient;
use crate::task::{Task, TaskStatus, TaskType};
use std::collections::HashMap;
use std::fs;
use std::{
    net::{IpAddr, Ipv6Addr},
    time::Duration,
};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::time::sleep;

const INTERMEDIATE_FOLDER: &str = "./intermediate/";
const FINAL_RESULT_FOLDER: &str = "./final/";

// Worker 是执行 Task 的机器的抽象
pub struct Worker {
    id: i32,
    client: Option<RpcClient>,
    map_fn: MapFn,
    reduce_fn: ReduceFn,
}

pub type MapFn = fn(String, String) -> Vec<(String, String)>;
pub type ReduceFn = fn(String, Vec<String>) -> (String, String);

// 目前好像不需要状态
// enum WorkerStatus {
//     Idle,
//     Running,
//     Offline,
// }

impl std::fmt::Display for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Worker {{ id: {}, client: {:?} }}", self.id, self.client)
    }
}

impl Worker {
    pub fn new(map_fn: MapFn, reduce_fn: ReduceFn) -> Self {
        Worker {
            id: 0,
            client: None,
            map_fn,
            reduce_fn,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        if self.client.is_some() {
            // already have a client running
            return Ok(());
        }

        // TODO: 端口不写死
        let server_addr: (IpAddr, u16) = (IpAddr::V6(Ipv6Addr::LOCALHOST), 50051);
        let mut transport = tarpc::serde_transport::tcp::connect(&(server_addr), Json::default);
        transport.config_mut().max_frame_length(usize::MAX);

        let client = RpcClient::new(client::Config::default(), transport.await?).spawn();
        self.client = Some(client);
        self.register().await?; // register to coordinator, obtain work_id
        println!("register complete: id is {}", self.id);
        Ok(())
    }

    async fn register(&mut self) -> anyhow::Result<()> {
        match &self.client {
            Some(client) => {
                let id = async move {
                    tokio::select! {
                        res = client.register_worker(context::current()) => { res }
                    }
                }
                .await;

                sleep(Duration::from_millis(500)).await;
                if let Ok(id) = id {
                    self.id = id;
                }

                Ok(())
            }
            None => Err(anyhow::anyhow!("Client not started")),
        }
    }

    async fn submit_task(&self, task: &Task) -> anyhow::Result<()> {
        match &self.client {
            Some(client) => {
                let _res = async move {
                    tokio::select! {
                        res = client.submit(context::current(), task.get_task_id(), task.get_task_output().clone()) => { res }
                    }
                }
                .await;

                sleep(Duration::from_millis(500)).await;
                Ok(())
            }
            None => Err(anyhow::anyhow!("Client not started")),
        }
    }

    // 一次 map 任务
    fn do_map(&self, input_files: &Vec<String>, task_id: i32) -> anyhow::Result<String> {
        let mut result: Vec<(String, String)> = vec![];
        let output = format!("{}/{}/{}.txt", INTERMEDIATE_FOLDER, self.id, task_id);
        for file in input_files {
            if let Ok(contents) = fs::read_to_string(file) {
                result.extend((self.map_fn)(file.clone(), contents));
            } else {
                println!("reading {} failed", file);
            }
        }
        match save_result(&result, &output) {
            Err(e) => println!("Error saving output to file: {}", e),
            _ => {}
        }
        Ok(output)
    }

    // 输入是此次 reduce 任务所需的所有中间文件（由 map 任务生成）
    fn do_reduce(&self, input_files: &Vec<String>, task_id: i32) -> anyhow::Result<String> {
        // read all map output
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        input_files.iter().for_each(|f| {
            for (k, v) in load_intermediate_result(f).unwrap_or(vec![]) {
                map.entry(k).or_insert(vec![]).push(v);
            }
        });

        let mut result: Vec<(String, String)> = vec![];
        for (k, v) in map {
            result.push((self.reduce_fn)(k, v));
        }

        let output = format!("{}/{}.txt", FINAL_RESULT_FOLDER, task_id);
        match save_result(&result, &output) {
            Err(e) => println!("Error saving output to file: {}", e),
            _ => {}
        }
        Ok(output)
    }

    async fn do_task(&self, task: Task) -> anyhow::Result<()> {
        println!("worker {} get task: {:?}", self.id, task);
        let mut task = task.clone();
        match task.get_task_type() {
            TaskType::Wait => {
                sleep(Duration::from_millis(500)).await;
            }
            TaskType::Map => {
                if let Ok(output) = self.do_map(task.get_input_files(), task.get_task_id()) {
                    task.set_task_output(Some(output));
                    task.set_status(TaskStatus::Done);
                    self.submit_task(&task).await?;
                } else {
                    task.set_status(TaskStatus::Failed);
                    self.submit_task(&task).await?;
                }
            }
            TaskType::Reduce => {
                if let Ok(output) = self.do_reduce(task.get_input_files(), task.get_task_id()) {
                    task.set_task_output(Some(output));
                    task.set_status(TaskStatus::Done);
                    self.submit_task(&task).await?;
                } else {
                    task.set_status(TaskStatus::Failed);
                    self.submit_task(&task).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.client {
            Some(client) => loop {
                let task = async move {
                    tokio::select! {
                        res = client.request_task(context::current(), self.id) => { res }
                    }
                }
                .await;

                match task {
                    Ok(task) => self.do_task(task).await?,
                    Err(e) => eprintln!("{:?}", e),
                }

                sleep(Duration::from_millis(500)).await;
            },
            None => return Err(anyhow::anyhow!("Client not started")),
        }
    }
}

fn save_result(result: &Vec<(String, String)>, path: &str) -> anyhow::Result<()> {
    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let serialized = serde_json::to_string(result)?;
    Ok(fs::write(path, serialized)?)
}

fn load_intermediate_result(path: &str) -> anyhow::Result<Vec<(String, String)>> {
    let contents = fs::read_to_string(path)?;
    let result: Vec<(String, String)> = serde_json::from_str(&contents)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_map_fn(_filename: String, contents: String) -> Vec<(String, String)> {
        contents
            .split_whitespace()
            .map(|word| (word.to_string(), "1".to_string()))
            .collect()
    }

    fn mock_reduce_fn(key: String, values: Vec<String>) -> (String, String) {
        let count = values.len();
        let res = (key, count.to_string());
        println!("{:?}", res);
        res
    }

    #[tokio::test]
    async fn test_worker_do_task() {
        let mut worker = Worker::new(mock_map_fn, mock_reduce_fn);
        worker.id = 1;
        let input_files = vec![
            "../resource/simple_test/1.txt".to_string(),
            "../resource/simple_test/2.txt".to_string(),
            "../resource/simple_test/3.txt".to_string(),
            "../resource/simple_test/4.txt".to_string(),
        ];
        let task = Task::new(1, input_files, TaskType::Map);
        let output = if let Ok(output) = worker.do_map(task.get_input_files(), task.get_task_id()) {
            let mut res = task.clone();
            res.set_task_output(Some(output));
            res
        } else {
            task
        };

        let output = output.get_task_output().clone().unwrap();

        let task = Task::new(2, vec![output], TaskType::Reduce);
        if let Ok(output) = worker.do_reduce(task.get_input_files(), task.get_task_id()) {
            let mut res = task.clone();
            res.set_task_output(Some(output));
            dbg!(res);
        } else {
            dbg!(task);
        }
    }
}
