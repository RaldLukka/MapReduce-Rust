use mr::coordinator::Coordinator;

fn main() {
    let input_files = vec![
        "../resource/simple_test/1.txt".to_string(),
        "../resource/simple_test/2.txt".to_string(),
        "../reduce/resource/simple_test/3.txt".to_string(),
        "../reduce/resource/simple_test/4.txt".to_string(),
    ];
    let n_map = 2;
    let n_reduce = 1;
    let coordinator = Coordinator::new(input_files.clone(), n_map, n_reduce);

    let _ = mr::coordinator::run(coordinator, 50051);
}
