use mr::worker::Worker;

fn map_function(_key: String, value: String) -> Vec<(String, String)> {
    let mut output = Vec::new();
    let words: Vec<&str> = value.split_whitespace().collect();

    for word in words {
        output.push((word.to_string(), "1".to_owned()));
    }

    return output;
}

fn reduce_function(key: String, values: Vec<String>) -> (String, String){
    let mut count = 0;
    for item in values {
        count += item.parse::<i32>().unwrap();
    }

    (key, count.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut worker = Worker::new(map_function, reduce_function);
    worker.start().await?;
    println!("worker started");
    worker.run().await?;
    println!("worker started: {}", worker);
    Ok(())
}