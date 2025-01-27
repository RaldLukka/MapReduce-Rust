use super::task::Task;

#[tarpc::service]
pub trait Rpc {
    async fn register_worker() -> i32;
    async fn request_task(id: i32) -> Task;
    async fn submit(task_id: i32, output: Option<String>);
    async fn heartbeat(work_id: i32);
}
