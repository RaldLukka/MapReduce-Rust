use futures::{future, lock::Mutex, prelude::*};
use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
};
use tarpc::{
    context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tokio::time::{sleep, Duration};

use crate::task::{Task, TaskStatus, TaskType};

#[derive(Debug, Clone)]
pub struct Coordinator {
    files: Vec<String>, // 输入文件
    n_map: i32,         // map 任务总数
    n_reduce: i32,      // reduce 任务总数
    tasks: Vec<Task>,
    worker_ids: Vec<i32>,
    reduce_completed: i32,
}

impl Coordinator {
    pub fn new(files: Vec<String>, n_map: i32, n_reduce: i32) -> Self {
        Coordinator {
            files,
            n_map,
            n_reduce,
            tasks: vec![],
            worker_ids: vec![],
            reduce_completed: 0,
        }
    }

    // 需要兼容多种分片策略，这里先只实现按照输入文件分，也不写 hash 分配了，假设
    fn slice(&mut self) {
        for i in 0..self.n_map {
            let mut input_files: Vec<String> = vec![];
            for (file_i, file) in self.files.iter().enumerate() {
                if file_i % self.n_map as usize == i as usize {
                    input_files.push(file.clone());
                }
            }
            // [1, n_map] 的 id 为 map task
            self.tasks
                .push(Task::new(i + 1, input_files, TaskType::Map));
        }
    }

    fn add_reduce_tasks(&mut self) {
        for i in 0..self.n_reduce {
            // (n_map, n_map + n_reduce] 的 id 为 reduce task
            let mut task = Task::new(i + 1 + self.n_map, vec![], TaskType::Reduce);
            task.set_status(TaskStatus::NotReady);
            self.tasks.push(task);
        }
    }

    fn add_input_to_reduce_task(&mut self, id: i32, input: String) {
        if let Some(i) = self
            .tasks
            .iter()
            .position(|task| task.get_task_id() == id && task.get_task_type() == TaskType::Reduce)
        {
            self.tasks[i].add_input_files(input);

            // 如果所有输入都已经准备好，设置状态为 Init
            let ready = self
                .tasks
                .iter()
                .filter(|task| {
                    task.get_task_type() == TaskType::Map
                        && task.get_task_id() % self.n_reduce == id
                })
                .all(|task| task.get_task_status() == TaskStatus::Done);
            if ready {
                self.tasks[i].set_status(TaskStatus::Init);
            }
        }
    }
}

#[derive(Clone)]
pub struct CoordinatorServer {
    coordinator: Arc<Mutex<Coordinator>>,
    socket_addr: SocketAddr,
}

impl CoordinatorServer {
    pub fn get_socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }
}

impl crate::rpc::Rpc for CoordinatorServer {
    async fn register_worker(self, _: tarpc::context::Context) -> i32 {
        let mut coordinator = self.coordinator.lock().await;
        let new_id = coordinator.worker_ids.last().map_or(0, |v| v + 1);
        coordinator.worker_ids.push(new_id);
        println!("got register request, res is {:?}", coordinator.worker_ids);
        new_id
    }

    async fn request_task(self, _: context::Context, id: i32) -> Task {
        sleep(Duration::from_millis(500)).await;
        let mut coordinator = self.coordinator.lock().await;
        if coordinator.worker_ids.contains(&id) {
            if let Some(i) = coordinator.tasks.iter().position(|task| {
                task.get_task_status() == TaskStatus::Init
                    || task.get_task_status() == TaskStatus::Failed
            }) {
                match coordinator.tasks[i].get_task_type() {
                    TaskType::Map => {
                        coordinator.tasks[i].set_worker_id(id);
                        coordinator.tasks[i].set_status(TaskStatus::Ongoing);
                        return coordinator.tasks[i].clone();
                    }
                    TaskType::Reduce => {
                        coordinator.tasks[i].set_worker_id(id);
                        coordinator.tasks[i].set_status(TaskStatus::Ongoing);
                        return coordinator.tasks[i].clone();
                    }
                    _ => Task::new_wait_task(),
                };
            } else {
                println!("No more tasks");
            }
        } else {
            println!("No such worker");
        }
        Task::new_wait_task()
    }

    async fn submit(self, _: context::Context, task_id: i32, output: Option<String>) {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(i) = coordinator
            .tasks
            .iter()
            .position(|task| task.get_task_id() == task_id)
        {
            // TODO: 先不处理失败的情况
            match coordinator.tasks[i].get_task_type() {
                TaskType::Map => {
                    if let Some(f) = output {
                        let i_reduce = task_id % coordinator.n_reduce + coordinator.n_map + 1;
                        coordinator.add_input_to_reduce_task(i_reduce, f);
                    }
                }
                TaskType::Reduce => {
                    coordinator.reduce_completed += 1;
                    if coordinator.reduce_completed == coordinator.n_reduce {
                        println!("All task completed.")
                    }
                }
                _ => {}
            }
        }
    }

    async fn heartbeat(self, _: context::Context, work_id: i32) {
        println!("worker {} send a heartbeat", work_id);
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
pub async fn run(coordinator: Coordinator, port: u16) -> anyhow::Result<()> {
    let server_addr: (IpAddr, u16) = (IpAddr::V6(Ipv6Addr::LOCALHOST), port);
    let coordinator = Arc::new(Mutex::new(coordinator));

    // JSON transport is provided by the json_transport tarpc module. It makes it easy
    // to start up a serde-powered json serialization strategy over TCP.
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
    println!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);
    let server_future = listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 2 per IP.
        .max_channels_per_key(2, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let server = CoordinatorServer {
                coordinator: Arc::clone(&coordinator),
                socket_addr: channel.transport().peer_addr().unwrap(),
            };
            channel
                .execute(crate::rpc::Rpc::serve(server))
                .for_each(spawn)
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {});

    // slice input
    coordinator.lock().await.slice();
    coordinator.lock().await.add_reduce_tasks();

    tokio::select! {
        _ = server_future => {},
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_coordinator_new() {
        let files = vec!["file1.txt".to_string(), "file2.txt".to_string()];
        let n_map = 2;
        let n_reduce = 2;
        let coordinator = Coordinator::new(files.clone(), n_map, n_reduce);

        assert_eq!(coordinator.files, files);
        assert_eq!(coordinator.n_map, n_map);
        assert_eq!(coordinator.n_reduce, n_reduce);
        assert!(coordinator.tasks.is_empty());
    }

    #[test]
    fn test_coordinator_slice() {
        let files = vec![
            "file1.txt".to_string(),
            "file2.txt".to_string(),
            "file3.txt".to_string(),
            "file4.txt".to_string(),
            "file5.txt".to_string(),
        ];
        let n_map = 3;
        let n_reduce = 2;
        let mut coordinator = Coordinator::new(files.clone(), n_map, n_reduce);

        coordinator.slice();

        assert_eq!(coordinator.tasks.len(), n_map as usize);

        let mut expected_tasks = vec![
            vec!["file1.txt".to_string(), "file4.txt".to_string()],
            vec!["file2.txt".to_string(), "file5.txt".to_string()],
            vec!["file3.txt".to_string()],
        ];

        for task in coordinator.tasks {
            assert!(expected_tasks.contains(&task.get_input_files()));
            expected_tasks.retain(|x| x != task.get_input_files());
        }
    }

    #[test]
    fn test_coordinator_slice_with_no_file() {
        let files = vec![];
        let n_map = 3;
        let n_reduce = 2;
        let mut coordinator = Coordinator::new(files.clone(), n_map, n_reduce);

        coordinator.slice();

        assert_eq!(coordinator.tasks.len(), n_map as usize);

        for task in coordinator.tasks {
            assert_eq!(task.get_input_files().len(), 0);
        }
    }
}
