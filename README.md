# MIT6.824

### 1. MapReduce

MapReduce是非常有影响力的一篇论文，发表于2004年，它第一次向人们传达了计算可扩展性这个概念，即通过增加机器来处理大规模数据的任务。具体而言，一次任务在MapReduce中的处理流程主要分成两个阶段：Map - Patition - Reduce。下面通过两个例子简单传达MapReduce的技术思想。

- 对一个大文件中关键词计数。首先把大文件分割，之后多个Map进程并行的在每个文件中，按照关键词的哈希将其分到不同位置，之后Reduce对其进行处理，合并得到最终结果。
- 全排序。要把一亿个处于$[0,2^{64})$区间的数字按照大小排序，多个Map进程并行的将数字按照大小Patition到不同的位置，例如$[0,2^{16})$、$[2^{16},2^{17})$、对应不同的Reduce任务，之后每个Reduce将它们排序，最后合并起来得到结果。

在上面两个例子中，如果修改一部分数据，哪怕一点点数据，都需要重新执行整个MapReduce过程来更新结果，处理搜索引擎爬虫的增量更新，不免捉襟见肘。后面会介绍Percolator，是Google的一个增量更新的数据库。

### 2. Google File System





### 3. BigTable






### 4. Fault-Tolerance Virtual Machine

VMware公司在2010年发表的企业级（enterprise-grade）容错虚拟机。

使用主从节点的思想，即把单台机器上执行的虚拟机状态复制到备份机器上。主要亮点，对性能没有太大的影响（不到10%），对主从机器之间的带宽要求不高（20MB/s）。

The primary/backup is a common approach to implementing fault-tolerant servers, where a backup server is always avaiable to take over if the primary server fails.

为了随时应对当前机器失效的情况，因此备份机器的状态在任何时候都需要与当前机器保持一致（nearly identical）。一种直观的方法是把对当前机器状态的虽有改动都复制到备份机器上，包括CPU、内存、IO，然而这种方法需要发送大量的数据，尤其是内存改动设涉及到很多数据，因此对带宽的要求十分高，不能实际使用。

更少占用带宽的另外一种方法是把虚拟机抽象成状态机，准确的说是确切（deterministic）的状态机（statemachine）。让当前机器和备份机器从相同的初始状态开始，接收相同的操作序列，其最终状态就是相同的。然而问题在于，虚拟机上大多数操作并不是deterministic的，因此就需要一些额外的协调（coordination）机制，当然额外的信息没有占用太多的代快，从量上来看起码大大少于内存的状态。

把虚拟机看作是抽象的状态机，这个状态机上的操作就是虚拟机（包括所有设备）上的操作。与物理机一样，虚拟机上面也有non-deterministic的操作，例如时间读取、中断分配等，因此需要向备份机器发送额外的信息以保持同步。虚拟机运行在hypervisor上，这给上述状态机的方案提供了很好的条件。好处就在于hypervisor拥有对虚拟机的执行控制权，包括所有输入信息，还有所有的关于non-deterministic信息，因此就可以在备份机器上重做这些操作。

这个虚拟机可以在当前机器失效后，立马切换到备份机器，然后继续往新的备份机器复制数据。当然，也存在一些问题，比如这些机器的处理器都是单核处理器，因为多核处理器带来一些共享内存的non-deterministic操作，因此带来很大的性能影响，这个工作仍在进展中。

对于non-deterministic操作，发送足够的信息使其成为deterministic操作，即使其有相同的状态改变和输出信息。对于non-deterministic事件，例如IO中断，发送事件发生时所处代码流的位置，备份机器上重做时在相同的位置执行代码流。

正式的说明容错需要达到的目的，即Output Requirement：如果备份虚拟机在主虚拟机发生故障后接管，则备份虚拟机将继续以与主虚拟机发送到外部世界的所有输出完全一致的方式继续执行。即Client对于这种故障切换是没有感知的。这非常简单，只需要当前机器把所有的输出延迟，只有当备份机器收到输出操作前的所有信息后，在输出给Client。然而，试想当前机器在执行完输出操作后故障，那么备份节点就需要知道它该重做日志，然后上线代替故障机器，此时就会出现一种情况，备份节点在执行那个输出操作之前，一些non-deterministic事件发生，例如中断/计时器等加入，就会被改变执行流进而不会执行上述输出操作。

备份机器和当前机器共享同一个硬盘，当发生网络分区时，两个机器不能相互通信，备份机器收不到对方的心跳包就要上线，但此时当前机器其实仍然处于上线状态，如果两者同时上线运行一定会出现问题，因此需要一定的机制避免这种情况。当备份机器上线之前需要test-and-set检测共享磁盘中的变量，如果不满足条件则持续等待。

当前机器与备份机器之间的Log通道以缓冲区的形式维护，如果备份机器重做的速度低于当前机器，那么Log通道一定会逐渐变满，当满了之后当前机器会被迫停止，等到有缓冲区时继续执行。此外，为了防止备份机器因为各种原因落后当前机器太多，会主动的调低当前机器的CPU频率来同步两个机器，当然这种情况并不是很多，不然会带来太大的性能损耗。

还要考虑到对当前机器的人为操作，例如关机，或者增加CPU频率等。也要通知到备份机器。

3.4和3.5章节的磁盘与网络IO的处理，没有细看。

接下来介绍了两个可选的其他实现方案。第一个是共享磁盘与非共享磁盘。对于非共享磁盘，当前机器对磁盘的写操作不必等到备份机器重做确认后再执行，并且也防止了共享磁盘故障后两者都不可恢复的情况。当然，也存在缺点，非共享磁盘必须通过某种方式同步磁盘，而且处理上述test-and-set操作时，还需要第三方服务器支持。第二个是备份机器是否从磁盘读数据，论文的方案是不从磁盘读数据，只重做Log通道的信息，从磁盘读数据一种显而易见的优势是可以减少带宽需求，但缺点是带来了性能损耗，例如备份机器需要等到读完数据才能往下执行，如果读磁盘失败还需要一种重复尝试读取。

最后是未来可能的研究方向，可以看到现在的方案只支持单核处理器，虽然作者声称单核处理器对于大多数工作场景已经绰绰有余，但多核处理器明显是接下来要做的，一种最简单的方法是横向扩展，即使用多个单核VM来处理任务。当前来看，高性能的多核处理器重做是一个活跃的研究领域。
