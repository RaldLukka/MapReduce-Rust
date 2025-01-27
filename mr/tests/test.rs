use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::thread;

enum TaskType {
    Map,
    Reduce,
}

struct Worker {
    task_type: TaskType,
}

struct Coordinator {
    n_map: usize,
    n_reduce: usize,
}

// key: line no, value: line content
fn read_input(path: &str) -> Vec<(String, String)> {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let mut result = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let content = line.unwrap();
        result.push(((line_no + 1).to_string(), content));
    }

    return result;
}

fn map_function(key: String, value: String) -> Vec<(String, String)> {
    let mut output = Vec::new();
    let words: Vec<&str> = value.split_whitespace().collect();

    for word in words {
        output.push((word.to_string(), "1".to_owned()));
    }

    return output;
}

fn reduce_function(key: String, values: Vec<String>) -> (String, String) {
    let mut count = 0;
    for item in values {
        count += item.parse::<i32>().unwrap();
    }

    return (key, count.to_string());
}

fn get_files(path: &str) -> io::Result<Vec<String>> {
    let entries = fs::read_dir(path)?;
    let mut result = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            result.push(path.to_str().unwrap().to_string());
        }
    }

    Ok(result)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = get_files("./resource/simple_test/")?;
    let n_map = 4;
    let n_reduce = 2;
    let mut intermediate_output: Vec<HashMap<String, Vec<String>>> = Vec::new();
    let mut input_for_worker: Vec<Vec<String>> = Vec::new();
    let mut input_for_reduce = Vec::new();

    // init
    for i in 0..n_map {
        let vec = Vec::new();
        input_for_worker.push(vec);
    }
    for i in 0..n_reduce {
        let map: HashMap<String, Vec<String>> = HashMap::new();
        input_for_reduce.push(map);
    }

    // add input
    for i in 0..files.len() {
        let tmp = input_for_worker.get_mut(i % n_map).unwrap();
        tmp.push(files.get(i).unwrap().to_string());
    }

    // do map
    let mut handles = vec![];
    for input in input_for_worker {
        let handle = thread::spawn(move || do_map(&input));

        handles.push(handle);
    }

    for handle in handles {
        let map = handle.join().unwrap();
        intermediate_output.push(map);
    }

    // merge intermediate
    for i_reduce in 0..n_reduce {
        for i in 0..intermediate_output.len(){
            if i_reduce == i % n_reduce {
                input_for_reduce[i_reduce] = merge_hashmap(
                    input_for_reduce.get(i_reduce).unwrap(),
                    intermediate_output.get(i).unwrap(),
                );
            }
        }
    }

    let mut reduce_handles = vec![];
    for input in input_for_reduce {
        let handle = thread::spawn(|| {
            let mut output = vec![];
            for (k, v) in input {
                output.push(reduce_function(k, v));
            }
            output
        });
        reduce_handles.push(handle);
    }

    for handle in reduce_handles {
        let result = handle.join().unwrap();
        dbg!(result);
    }

    return Ok(());
}

fn do_map(input: &Vec<String>) -> HashMap<String, Vec<String>> {
    let mut map = HashMap::new();

    for file in input {
        let input_for_map = read_input(file);
        for line in input_for_map {
            let map_output = map_function(line.0, line.1);
            for (k, v) in map_output {
                map.entry(k).or_insert(vec![]).push(v);
            }
        }
    }

    map
}

fn merge_hashmap(
    map1: &HashMap<String, Vec<String>>,
    map2: &HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let mut merged_map = map1.clone();

    for (key, values) in map2 {
        merged_map
            .entry(key.to_string())
            .or_insert(Vec::new())
            .extend(values.clone());
    }

    merged_map
}