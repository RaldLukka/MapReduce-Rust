# MapReduce 的 Rust 实现

## 简要说明

Rust 版本的 Mapreduce，分为两个部分，mr 是 mapreduce 的实现。mr_app 是一个基于 mr 的具体应用：word count。

## 过程记录

### rpc 的基本调用流程

使用 tarpc 包。

应该是 client 向 server 发送请求。在 mapreduce 中，是 worker 向 coordinator 请求任务。

通过一个示例，渐渐明白 rpc 的使用了。server 端需要实现自定义的 trait，这些 trait 就是 client 可以调用的方法，server 端来具体实现。client 端只要调用方法就可以了。

```rust
// 这个标注告诉 Rust 这是一个 rpc 服务
#[tarpc::service]
pub trait Rpc {
    async fn request_task(id: i32) -> Task;
    async fn send_complete_signal(id: i32) -> Task;
    async fn heartbeat(id: i32) -> Task;
}

// client 可以直接调用 xxxClient 生成
let client = RpcClient::new(client::Config::default(), transport.await?).spawn();
```

### 输入分片步骤的详细学习

> In practice, we tend to choose M so that each individual task is roughly 16 MB to 64 MB of input data (so that the locality optimization described above is most effective),  and we make R a small multiple of the number of worker machines we expect to use.

这里就说明 M 和 R 都要大于 worker 的数量，也就是说 worker 要执行多个任务。

代码中分片的实现实际是分文件。后续如果改成分块的话，可以改成每块生成一个文件，这样数据结构其实不变。

### Rust 项目的文件结构

目前使用 Cargo 的工作空间特性，允许在一个文件夹下创建多个 crate。我们将一个作为 mapreduce 的 lib，另一个作为使用该 lib 的程序实现。

总体的 `Cargo.toml`

```toml
[workspace]
members = ["mr", "mr_app"]
resolver = "2"
```

每个子文件夹内，调用 `cargo init` 创建 crate，然后再添加依赖等信息。

### server 实现

先不管如何关闭 server。

需要把 coordinator 和 rpc 服务端放到一起。还要保证服务端的 rpc 方法可以访问并修改 coordinator 中的数据。这里采取的方法是 `CoordinatorServer` 结构体中，增加 `Arc<Mutex<Coordinator>>` 的成员：一个使用 `Arc<T>` 包装的 `Mutex<T>`。能够实现在多线程之间共享所有权。

下一步需要实现的是将 rpc 服务置于后台，让 rpc 服务和其他 Coordinator 服务并行（例如 slice）。

> `future trait` 是 Rust 标准库中的一个异步任务接口，表示一个可能尚未完成但是可以等待其完成的计算。

### 中间态文件存在哪？

目前的想法是保存在文件夹中，每个 worker 一个文件。

## TODO

有什么功能没有实现？

1. 组合器函数
2. backup tasks

## 输出

对 vec 使用 get，返回值是一个 Option，而且里面的值是指针，因此直接返回这个值就会出错。
