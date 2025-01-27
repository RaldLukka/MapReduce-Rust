#[derive(Debug, PartialEq, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    NotReady,
    Init,
    Ongoing,
    Done,
    Failed,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub enum TaskType {
    Wait,
    Map,
    Reduce,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Task {
    id: i32,
    input_files: Vec<String>,
    status: TaskStatus,
    task_type: TaskType,
    worker_id: Option<i32>,
    output: Option<String>,
}

impl Task {
    pub fn new(id: i32, input_files: Vec<String>, task_type: TaskType) -> Task {
        Task {
            id,
            input_files,
            status: TaskStatus::Init,
            task_type,
            worker_id: None,
            output: None,
        }
    }

    pub fn new_wait_task() -> Task {
        Task {
            id: 0, // 0 means not used
            input_files: vec![],
            status: TaskStatus::Init,
            task_type: TaskType::Wait,
            worker_id: None,
            output: None,
        }
    }

    pub fn set_input_files(&mut self, input_files: Vec<String>) {
        self.input_files = input_files;
    }

    pub fn add_input_files(&mut self, input_file: String) {
        self.input_files.push(input_file);
    }

    pub fn get_input_files(&self) -> &Vec<String> {
        &self.input_files
    }

    pub fn get_task_type(&self) -> TaskType {
        self.task_type.clone()
    }

    pub fn get_task_status(&self) -> TaskStatus {
        self.status
    }

    pub fn set_worker_id(&mut self, id: i32) {
        self.worker_id = Some(id);
    }

    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }

    pub fn get_task_id(&self) -> i32 {
        self.id
    }

    pub fn get_task_output(&self) -> &Option<String> {
        &self.output
    }

    pub fn set_task_output(&mut self, output: Option<String>) {
        self.output = output;
    }
}
