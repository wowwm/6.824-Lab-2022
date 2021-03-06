package raft

//
// 这是 Raft 需要暴露的 API 概要，更多细节需要自行补充
//
// rf = Make(...)
//   创建一个新 Raft 服务节点
// rf.Start(command interface{}) (index, term, isleader)
//   开始处理附加新日志条目
// rf.GetState() (term, isLeader)
//   询问 Raft 当前任期，已经它是否认为自己是 Leader
// ApplyMsg
//   每次提交新日志条目时，每个节点都应向同一服务器中的服务者/测试者发送 ApplyMsg
//

import (
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// 当每个节点知道日志条目已提交，应向同一服务器上的服务/测试者发 ApplyMsg，通过 applyCh 通道传递给 Make()。
// 设置 CommandValid 为 true 表示此 ApplyMsg 包含一个新的已提交日志条目。
//
// 在 2D 部分需要发送其他消息到 applyCh，例如快照，此时将 CommandValid 设为 false 表示用于其他用途。
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//2D 部分使用的参数:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft 对象的结构体.
type Raft struct {
	mu        sync.Mutex          // Lock 保护对此 peer 状态的共享访问
	peers     []*labrpc.ClientEnd // 所有 peers 的 RPC 端点
	persister *Persister          // 保持此 peer 持久状态的对象
	me        int                 // 这个 peer 在 peers[] 的索引
	dead      int32               // Kill() 设置，标识是否存活

	// 自行添加更多参数 (2A, 2B, 2C).
	// 查看论文中的图2，了解 Raft 节点需要的状态

	// 所有服务器上持久存在的（在响应RPCs之前已在稳定的存储上进行更新）
	currentTerm int   // 服务器最后⼀次知道的任期号（初始化为 0，持续递增）
	votedFor    int   // 在当前获得选票的候选⼈的 Id
	logs        []Log // ⽇志条⽬集；每⼀个条⽬包含⼀个⽤户状态机执⾏的指令，和收到时的任期号
	// 所有服务器上经常变的
	commitIndex int // 已知的最⼤的已经被提交的⽇志条⽬的索引值
	lastApplied int // 最后被应⽤到状态机的⽇志条⽬索引值（初始化为 0，持续递增）
	// 在 Leader ⾥经常改变的（选举后重新初始化）
	nextIndex  []int // 对于每⼀个服务器，需要发送给他的下⼀个日志条⽬的索引值（初始化为领导⼈最后索引值加⼀）
	matchIndex []int // 对于每⼀个服务器，已经复制给他的⽇志的最⾼索引值

	voteTimeout bool // 选举超时标记，收到 RPC 就置为 false
	state       int  // 当前状态，0 follower，1 candidate，2 leader

	applyCh chan ApplyMsg //
}

// Log 日志条目结构体
type Log struct {
	Command interface{} // 执⾏的指令
	LogTerm int         // 收到时的任期号
}

// GetState 返回 Raft 当前任期，已经它是否认为自己是 Leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == 2
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// 保存 Raft 的持久状态到持久储存中，崩溃后可以在其中检索并重新启动
// 查看论文图 2，了解应该持久化的内容
//
func (rf *Raft) persist() {
	// 用于 (2C).
	// 例子:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// 恢复崩溃前的持久状态
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// 用于 (2C).
	// 例子:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// 一个服务想要切换到快照，只有在它传快照到 applyCh 以来，没有更新的信息时才这样做
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// 用于 (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 用于 (2D).

}

// AppendEntriesArgs AppendEntries RPC 参数结构
type AppendEntriesArgs struct {
	Term         int   // 领导⼈的任期号
	LeaderId     int   // 领导⼈的 Id，以便于跟随者重定向请求
	PrevLogIndex int   // 新的⽇志条⽬紧随之前的索引值
	PrevLogTerm  int   // prevLogIndex 条⽬的任期号
	Entries      []Log // 准备存储的⽇志条⽬（表示⼼跳时为空；⼀次性发送多个是为了提⾼效率）
	LeaderCommit int   // 领导⼈已经提交的⽇志的索引值
}

// AppendEntriesReply AppendEntries RPC 回复结构
type AppendEntriesReply struct {
	Term         int  // 当前的任期号，⽤于领导⼈去更新⾃⼰
	Success      bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的⽇志时为真
	LastLogIndex int  // 最后复制成功日志的索引
}

// RequestVoteArgs RequestVote RPC 参数结构.
// 字段名必须以大写字母开头
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	候选⼈的任期号
	CandidateId  int // 请求选票的候选⼈的 Id
	LastLogIndex int // 候选⼈的最后⽇志条⽬的索引值
	LastLogTerm  int // 候选⼈最后⽇志条⽬的任期号
}

// RequestVoteReply
// RequestVote RPC 回复结构.
// 字段名必须以大写字母开头
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选⼈去更新⾃⼰的任期号
	VoteGranted bool // 候选⼈赢得了此张选票时为 true
}

// RequestVote RPC 处理程序.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.state == 0 && args.Term >= rf.currentTerm { // follower 收到有效 RPC 即重置超时
		rf.voteTimeout = false
	}
	if args.Term > rf.currentTerm { // 比较任期号，若过时则更新并转为 follower
		//DPrintf("raft %d 收到请求投票发现任期过时", rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 重置投票
		rf.state = 0
		rf.voteTimeout = false
	}
	reply.Term = rf.currentTerm

	// 判断日志新旧
	logFlag := true
	myLogIndex := rf.matchIndex[rf.me]
	myLogTerm := 0
	if myLogIndex != 0 {
		myLogTerm = rf.logs[myLogIndex-1].LogTerm
	}
	if args.LastLogTerm < myLogTerm {
		logFlag = false
	} else if args.LastLogTerm == myLogTerm {
		if args.LastLogIndex < myLogIndex {
			logFlag = false
		}
	}

	// 如果任期比自己小，或者已投票，或者日志没有自己新，拒绝投票
	if args.Term < rf.currentTerm || rf.votedFor != -1 || !logFlag {
		reply.VoteGranted = false
		DPrintf("raft %d 拒绝给 raft %d 投票", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("raft %d 投票给了 raft %d", rf.me, rf.votedFor)
	}
	rf.mu.Unlock()
}

//
// 发送 RequestVote RPC 到服务器的示例代码
// server 是在 rf.peers[] 中目标服务器的索引.
// 需要 RequestVoteArgs RPC 参数.
// *reply 即 RPC 回复
// 传递给 Call() 的参数和回复类型必须与处理函数中声明的参数类型相同（包括指针）
//
// labrpc 包模拟有损网络，其中服务器可能无法访问，请求和回复可能会丢失。
// Call() 发送请求并等待回复。 如果回复在超时间隔内到达，则 Call() 返回 true；
// 否则 Call() 返回 false。 因此 Call() 可能不会立即返回。
// 错误返回可能由死服务器、无法访问的活动服务器、丢失的请求或丢失的回复引起。
//
// Call() 保证返回（可能在延迟之后），除非服务器端的处理函数没有返回。
// 因此，无需围绕 Call() 实现您自己的超时。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息。
//
// 如果您无法让 RPC 工作，请检查您是否已将通过 RPC 传递的结构中的所有字段名称大写，
// 并且调用者使用 & 传递了回复结构的地址，而不是结构本身。
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("raft %d 请求 raft %d 投票", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC 处理程序.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("raft %d state = %d 收到来自 %d 的 AE PRC，args=%v", rf.me, rf.state, args.LeaderId, args)
	reply.Term = rf.currentTerm
	reply.Success = true // 默认回复 true

	if args.Term < rf.currentTerm {
		DPrintf("raft %d state=%d 发现来自 %d 的 AE RPC 过时，返回 false，term = %d", rf.me, rf.state, args.LeaderId, rf.currentTerm)
		reply.Success = false
	} else {
		//DPrintf("raft %d 收到正常 leader AE RPC，重置", rf.me)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			//reply.Term = rf.currentTerm	// 其实不需要对更新的 term 进行回应
		}
		if rf.state != 0 { // 若不是 follower
			rf.votedFor = -1 // 重置投票
			rf.state = 0     // 重置为 follower
		}
		rf.voteTimeout = false // 重置投票超时

		// TODO 日志处理 这里试试心跳不进行处理
		if len(args.Entries) != 0 {
			DPrintf("raft %d 开始日志处理，log=%v，Entries=%v", rf.me, rf.logs, args.Entries)
			// FIXME 这里的前提是 follower 的日志序号不会比 Leader 已知的小
			if len(rf.logs) == 0 {
				// 直接追加
				rf.logs = make([]Log, 0)
				rf.logs = append(rf.logs, args.Entries...)
				rf.nextIndex[rf.me] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[rf.me] = args.PrevLogIndex + len(args.Entries)
				DPrintf("raft %d 追加第一份日志成功，logs = %v", rf.me, rf.logs)
			} else {
				if args.PrevLogIndex != 0 && (rf.matchIndex[rf.me] < args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].LogTerm) {
					//fmt.Println(rf.matchIndex[rf.me], args.PrevLogIndex, args.PrevLogTerm, rf.logs)
					reply.Success = false // 上次日志任期不匹配，返回 false
					DPrintf("raft %d 发现上次日志任期不匹配，logs=%v，返回 false", rf.me, rf.logs)
				} else {
					// 如果现有的⽇志条⽬和新的产⽣冲突（索引相同任期号不同），删除现有的和之后所有的条目
					for i, entry := range args.Entries {
						// 5,2
						if len(rf.logs) <= args.PrevLogIndex+i {
							rf.logs = append(rf.logs, args.Entries[i:]...)
							break
						} else if rf.logs[args.PrevLogIndex+i].LogTerm != entry.LogTerm {
							rf.logs = rf.logs[:args.PrevLogIndex+i]
							rf.logs = append(rf.logs, args.Entries[i:]...)
							break
						}
					}
					rf.nextIndex[rf.me] = len(rf.logs) + 1
					rf.matchIndex[rf.me] = len(rf.logs)
					DPrintf("raft %d 追加日志成功,nextIndex = %d matchIndex = %d,logs=%v", rf.me, rf.nextIndex[rf.me], rf.matchIndex[rf.me], rf.logs)

				}
			}
			reply.LastLogIndex = rf.matchIndex[rf.me] // 更新回复最后的索引
		}
		// 检查 Leader 提交情况
		if args.LeaderCommit > rf.commitIndex && rf.matchIndex[rf.me] == args.LeaderCommit && rf.logs[args.LeaderCommit-1].LogTerm == args.Term {
			for i := rf.commitIndex; i < args.LeaderCommit; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i + 1,
				}
				DPrintf("raft %d 检测到新的日志提交，提交日志，applyMsg=%v", rf.me, applyMsg)
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = args.LeaderCommit
		}
	}

}

// 发送 AppendEntries RPC 到服务器
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start 2B
// Raft 开始处理以将命令附加到复制的⽇志中，应该⽴即返回，⽽⽆需等待⽇志附加完成。
// 使用 Raft 的服务（例如一个 k/v 服务器）想要就下一个要附加到 Raft 日志的命令开始协议。
// 如果此服务器不是领导者，则返回 false。 否则启动协议并立即返回。
// 无法保证此命令将永远提交到 Raft 日志，因为领导者可能会失败或失去选举。
// 即使 Raft 实例被杀死，这个函数也应该优雅地返回。
//
// 第一个返回值是该命令在提交时将出现的索引。 第二个返回值是当前任期。
// 如果此服务器认为它是领导者，则第三个返回值为 true。
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	var isLeader bool
	var term int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs) + 1 // 命令在日志的索引  // FIXME 确定 Index 到底怎么取，这里让索引从1开始
	state := rf.state
	if state == 2 { // 	只有 Leader 回应 true
		DPrintf("----------------------------------")
		DPrintf("客户端请求 leader raft %d command=%v", rf.me, command)
		isLeader = true
		// 客户端的请求添加到日志
		term = rf.currentTerm
		log := Log{
			Command: command,
			LogTerm: rf.currentTerm,
		}
		rf.logs = append(rf.logs, log)
		// 更新自己的 nextIndex matchIndex
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		DPrintf("Leader 将 command 添加到日志")
		// 发 AE RPC
		peers := rf.peers
		leaderId := rf.me
		prevLogIndex := 0 // 循环里再更新
		prevLogTerm := 0
		leaderCommit := rf.commitIndex
		for serverID, _ := range peers {
			if serverID != leaderId {
				// 每个 follower 需要的下一个条目不一样, 用 nextIndex 维护
				if rf.matchIndex[serverID] > 0 {
					prevLogIndex = rf.matchIndex[serverID] // FIXME 这里让索引从 1 开始
					prevLogTerm = rf.logs[prevLogIndex-1].LogTerm
				}
				entries := rf.logs[prevLogIndex:]
				aeArgs := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				DPrintf("Leader %d 发 AE RPC 给 %d，PrevLogIndex = %v aeArgs = %v", rf.me, serverID, prevLogIndex, aeArgs)
				go rf.leaderAERPC(serverID, aeArgs)
			}
		}
	}
	DPrintf("raft %d 返回给应用层的：index = %d, term = %d, isLeader = %v", rf.me, index, term, isLeader)
	return index, term, isLeader
}

// Kill
// 测试器不会在每次测试后停止由 Raft 创建的 goroutine，但它会调用 Kill() 方法。
// 您的代码可以使用kill() 来检查是否调用了Kill()。 atomic 的使用避免了对锁的需要。
//
// 问题是长时间运行的 goroutine 会占用内存并且可能会占用 CPU 时间，可能会导致以后的测试失败并产生令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 都应该调用 kill() 来检查它是否应该停止。
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 如果这个节点最近没有收到心跳，ticker goroutine 开始一个新的选举。
func (rf *Raft) ticker() {
	DPrintf("raft %d 检查选举循环开始", rf.me)
	for rf.killed() == false {
		// 你的代码在这里检查是否应该开始领导选举并使用 time.Sleep() 随机化睡眠时间。
		rand.Seed(time.Now().UnixNano())
		sleepTime := rand.Intn(500) + 500
		time.Sleep(time.Millisecond * time.Duration(sleepTime))
		rf.mu.Lock()
		isTimeout := rf.voteTimeout
		state := rf.state
		rf.mu.Unlock()

		if state == 2 {
			DPrintf("raft %d 此时为 leader，结束选举循环", rf.me)
			break // 是 leader 则结束选举循环
		}

		if isTimeout { // 选举超时
			// FIXME 这里看需不需要加个 WaitGroup
			go rf.candidateDo()
		} else { // 选举未超时
			//DPrintf("raft %d 检查选举未超时", rf.me)
			rf.mu.Lock()
			rf.voteTimeout = true
			rf.mu.Unlock()
		}
	}
}

// candidateDo 要实现的请求投票
func (rf *Raft) candidateDo() {
	rf.mu.Lock()
	rf.currentTerm++ // 自增 term
	rf.votedFor = -1 // 重置投票
	DPrintf("raft %d 选举超时，成为candidate。currentTerm = %d", rf.me, rf.currentTerm)
	rf.state = 1           // 成为 candidate 开始选举
	rf.votedFor = rf.me    // 给自己投票
	rf.voteTimeout = false // 重置选举超时计时器
	voteNum := 1           // 得票数
	peers := rf.peers
	me := rf.me
	currentTerm := rf.currentTerm
	lastLogIndex := rf.matchIndex[rf.me]
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].LogTerm
	}
	rf.mu.Unlock()

	// 请求投票
	DPrintf("candidate %d 开始请求投票", rf.me)
	wg := sync.WaitGroup{}
	wg.Add(len(peers) - 1) // 不包括自己
	for server, _ := range peers {
		if server != me {

			// 并发发请求投票，不要在 RPC 时持有锁
			go func(server int) {
				defer wg.Done()
				rvArgs := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rvReply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, rvArgs, rvReply)
				if !ok {
					//DPrintf("raft %d 请求 %d 投票失败", rf.me, server)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != 1 { // 不是 candidate 了请求投票直接结束
					//DPrintf("raft %d 已不是 candidate，不再处理来自 %d 回复", rf.me, server)
					return
				}
				if rvReply.Term > rf.currentTerm { // 自己的任期过期，更新任期，回到 follower
					rf.currentTerm = rvReply.Term
					rf.votedFor = -1 // 重置投票
					rf.state = 0
					rf.voteTimeout = false
					DPrintf("candidate %d 成为 follower，任期过期", rf.me)
				} else {
					if rvReply.VoteGranted { // 如果被投票
						voteNum++
						// 票数过半，成为 leader，启动 leader 循环
						if voteNum > len(rf.peers)/2 {
							rf.state = 2
							//DPrintf("candidate %d 成为 leader state=%d，当前任期 %d", rf.me, rf.state, rf.currentTerm)
							go rf.leaderDo()
						}
					}
				}
			}(server)
		}
	}
	wg.Wait()
	DPrintf("candidate %d 请求投票结束", rf.me)
}

// leader 要实现的功能
func (rf *Raft) leaderDo() {
	DPrintf("leader %d 开始工作循环，当前任期 %d", rf.me, rf.currentTerm)
	// 每次新 Leader 初始化 nextIndex 和 matchIndex
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))  // 对于每⼀个服务器，需要发送给他的下⼀个⽇志条⽬的索引值（初始化为领导⼈最后索引值加⼀）
	rf.matchIndex = make([]int, len(rf.peers)) // 对于每⼀个服务器，已经复制给他的⽇志的最⾼索引值
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = len(rf.logs)
	}
	rf.mu.Unlock()
	//上任后先提交一个空命令
	//rf.Start(nil)

	for rf.killed() == false { // 成为 leader 后一直在这个循环
		rf.mu.Lock()
		if rf.state != 2 { // 卸任 leader 后启动 ticker
			DPrintf("raft %d 失去 leader 身份，leaderDo 循环结束，启动请求投票循环 ticker", rf.me)
			go rf.ticker()
			rf.mu.Unlock()
			return
		}
		peers := rf.peers
		term := rf.currentTerm
		leaderId := rf.me
		prevLogIndex := 0
		prevLogTerm := 0
		// FIXME 心跳不进行处理 这里就不需要了
		//if len(rf.logs) > 0 { // 有日志后
		//	prevLogIndex = len(rf.logs) // FIXME 确定 Index 到底怎么取，这里让索引从 1 开始
		//	prevLogTerm = rf.logs[prevLogIndex-1].LogTerm
		//}
		entries := make([]Log, 0)
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()
		aeArgs := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		//DPrintf("raft %d leader 心跳", rf.me)
		// 发一次心跳
		for serverID, _ := range peers {
			if serverID != leaderId {
				go rf.leaderAERPC(serverID, aeArgs)
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

// 用于 Leader 发一次 AE RPC，使用协程
func (rf *Raft) leaderAERPC(server int, aeArgs *AppendEntriesArgs) {
	aeReply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, aeArgs, aeReply)
	if !ok {
		term, isLeader := rf.GetState()
		if isLeader && term == aeArgs.Term {
			go rf.leaderAERPC(server, aeArgs)
			return
			//ok2 := rf.sendAppendEntries(server, aeArgs, aeReply)
			//if !ok2 {
			//	DPrintf("raft %d 发往 %d 的 AE RPC 失败", rf.me, server)
			//}
		}
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 处理 AE RPC 的返回值
	//DPrintf("leader %d term=%d 收到来自 raft %d 的回复 %v", rf.me, rf.currentTerm, server, aeReply)
	if aeReply.Success == false {
		if aeReply.Term > rf.currentTerm { // 任期过期变为 follower
			DPrintf("leader %d 收到 AE RPC 回复发现任期过期，变为 follower", rf.me)
			rf.currentTerm = aeReply.Term
			rf.votedFor = -1
			rf.state = 0
			rf.voteTimeout = false
		} else {
			if rf.state != 2 {
				return // 检查自己还是不是 Leader
			}
			// 处理 success 日志不一致情况
			if rf.matchIndex[server]-1 < 0 {
				return
			}
			rf.nextIndex[server]--
			rf.matchIndex[server]--
			aeArgs.Entries = rf.logs[rf.matchIndex[server]:]
			aeArgs.PrevLogIndex = rf.matchIndex[server]
			aeArgs.PrevLogTerm = rf.logs[aeArgs.PrevLogIndex-1].LogTerm
			DPrintf("leader %d 收到 raft %d 日志不一致，减小 nextIndex 重发 %v", rf.me, server, aeArgs)
			go rf.leaderAERPC(server, aeArgs)
		}
	} else {
		if len(aeArgs.Entries) != 0 {
			// TODO 追加成功
			DPrintf("leader %d 收到来自 raft %d 的回复 %v，追加成功", rf.me, server, aeReply)
			if aeReply.LastLogIndex > rf.matchIndex[server] {
				rf.nextIndex[server] = aeReply.LastLogIndex + 1
				rf.matchIndex[server] = aeReply.LastLogIndex
			}
			DPrintf("leader %d 更新：rf.nextIndex[%d] = %d rf.matchIndex[%d] = %d， matchIndex=%v", rf.me, server, rf.nextIndex[server], server, rf.matchIndex[server], rf.matchIndex)
			go rf.commitEntries() // 检查是否可以提交
		}
	}
}

func (rf *Raft) commitEntries() {
	eMap := map[int]int{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex == rf.matchIndex[rf.me] {
		return // 已经提交过了
	}
	DPrintf("此时 rf.matchIndex = %d", rf.matchIndex)
	for _, v := range rf.matchIndex {
		eMap[v]++
	}
	for k := range eMap {
		if k > rf.commitIndex && eMap[k] > len(rf.peers)/2 && rf.logs[k-1].LogTerm == rf.currentTerm {
			for i := rf.commitIndex; i < k; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i + 1,
				}
				DPrintf("leader 给应用层发 applyMsg = %v", applyMsg)
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = k
			DPrintf("leader %d 更新 commitIndex = %d，eMap = %v", rf.me, k, eMap)
		}
	}
}

// Make
// 服务/测试者想要创建一个 Raft 服务器. 所有 Raft 节点的端口都在 peers[]
// 当前节点的端口为 peers[me]. 所有服务器的 peers[] 顺序相同.
// persister 是此服务器保存其持久状态的地方，并且最初还保存最近保存的状态（如果有）。
// applyCh 是测试人员或服务期望 Raft 发送 ApplyMsg 消息的通道。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{} // Raft 对象的引用
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A)
	rf.currentTerm = 0    // 初始化当前任期
	rf.voteTimeout = true // 初始化超时标记
	rf.votedFor = -1      // -1 表示未投票，每个新任期重置

	// Your initialization code here (2B)
	// 注意数组索引从 0 开始，日志索引从 1 开始
	rf.commitIndex = 0                      // 已知的最⼤的已经被提交的⽇志条⽬的索引值
	rf.lastApplied = 0                      // 最后被应⽤到状态机的⽇志条⽬索引值
	rf.nextIndex = make([]int, len(peers))  // 对于每⼀个服务器，需要发送给他的下⼀个⽇志条⽬的索引值（初始化为领导⼈最后索引值加⼀）
	rf.matchIndex = make([]int, len(peers)) // 对于每⼀个服务器，已经复制给他的⽇志的最⾼索引值
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		//rf.matchIndex[i] = 0
	}
	rf.applyCh = applyCh

	// 从崩溃前持续的状态初始化
	rf.readPersist(persister.ReadRaftState())

	DPrintf("初始化 raft %d，开启 ticker", rf.me)
	// 启动 ticker goroutine，一段时间没收到心跳后开始选举
	go rf.ticker()

	return rf
}
