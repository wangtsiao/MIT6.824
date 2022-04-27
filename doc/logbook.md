## 准备工作

在课程网站中推荐使用G1.15，但是在我的电脑上使用go的plugin功能时会出现无法加载，或者无法编译plugin的情况。

接着我看了下src/main/test-mr.sh这个文件，其中指明在macos系统上go1.17 before 1.17.6有时会出现崩溃情况。

因此我决定使用go1.18版本，经过测试一切正常。下面记录一下安装go1.18版本以及在`Goland`中配置`GROOT`所遇到的问题。

通过下述命令安装，注意需要使用到代理，这可以通过设置http和https环境变量解决。

```bash
go install golang.org/dl/go1.18@latest
go1.18 download
```

接着在路径`~/go/sdk`中可以看到安装的特定版本的`go`文件夹，命令行`go1.18 env GOROOT`可以看到路径。

接着在`Goland`中设置`GOROOT`出现错误，`The selected directory is not a valid home for Go SDK`，这是`Goland`的一个问题。

我参考了[这个讨论](https://youtrack.jetbrains.com/issue/GO-11588#focus=Comments-27-5127829.0-0) 中的方法解决了问题。

![](GoLand-Issue.png)

一切准备就绪，接下来可以进入MIT6.824课程实验正题了。

## MapReduce

```bash
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrsequential.go wc.so pg*.txt
```
上述第二条命令是编译`wc.go`为共享库，在go语言中的表现形式是plugin。接着第三条命令加载它以MapReduce的方式运行代码获得结果。

本个lab的任务是通过test-mr.sh的测试，所以我要阅读一下其中的代码。在阅读代码过程中，我发现了这段：`> /dev/null 2>&1`。

其中第一个`> /dev/null`表示将命令的输出重定向到`/dev/null`，接着`2>&1`表示把标准错误与标准输出融合在一起，其中`&`表现后面的是文件描述符，
而非文件名。

在MacOS默认是没有timeout命令的，可以通过`brew install coreutils`来使用`gtimeout`命令。

补充说明一点关于`GO RPC`的疑问，在接收RPC并不是队列的形式，而是每接收一个请求，就`go`一个线程，因此不存在等待的问题，同时也需要注意多线程编程加锁同步。

