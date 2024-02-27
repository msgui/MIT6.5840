package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new fmt entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the fmt, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive fmt entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed fmt entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int

const (
	Follower  Status = 1
	Candidate Status = 2
	Leader    Status = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	receiveTime int64
	status      Status
	applyCh     *chan ApplyMsg
	nextLock    sync.Mutex

	fmts []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the fmt through (and including)
// that index. Raft should now trim its fmt as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastfmtIndex int
	LastfmtTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// example RequestVote RPC handler.

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
var ss = getNowTimeMilli()

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's fmt. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft fmt, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := rf.status == Leader

	// Your code here (2B).

	if isLeader == false || rf.killed() {
		return index, term, isLeader
	}

	fmtEntry := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.fmts = append(rf.fmts, fmtEntry)

	return len(rf.fmts) - 1, rf.currentTerm, rf.status == Leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.status != Leader && rf.timeOutCheck() {
			if rf.election(rf.currentTerm) {
				continue
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 120 + (rand.Int63() % 301)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		if rf.status == Leader {

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					preIndex := rf.nextIndex[i] - 1

					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PreLogIndex:  preIndex,
						PreLogTerm:   rf.fmts[preIndex].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					reply := &AppendEntriesReply{}

					if rf.status != Leader {
						return
					}
					rf.sendAppendEntries(i, args, reply)
					if reply.Term > rf.currentTerm {
						if rf.status == Leader {
							rf.mu.Lock()
							if rf.status == Leader {
								rf.status = Follower
								rf.updateRecTime()
								fmt.Printf("当前id: %d, term: %d, Leader降级为Follwer\n", rf.me, rf.currentTerm)
							}
							rf.mu.Unlock()
						}
					} else {
						if !reply.Success {
							rf.nextIndex[i] = max(1, rf.nextIndex[i]-1)
						}
					}
				}(i)
			}

			rf.updateCommitIdx()
		}
		ms := 47 + rand.Int63()%14
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election(oldTerm int) bool {
	ret := false
	rf.mu.Lock()
	if oldTerm < rf.currentTerm {
		rf.updateRecTimeAndUnLock()
		return false
	}
	if !rf.timeOutCheck() {
		rf.mu.Unlock()
		return true
	}
	rf.status = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.updateRecTime()
	rf.mu.Unlock()

	fmt.Printf("Election超时  , 当前term: %d, id: %d, 时间: %d\n", currentTerm, rf.me, getNowTimeMilli()-ss)
	args := RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}
	votedForMe := atomic.Int64{}
	votedForMe.Add(1)
	wait := atomic.Int64{}
	maxTerm := currentTerm
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wait.Add(1)
		go func(i int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)
			if reply.VoteGranted {
				votedForMe.Add(1)
				if reply.Term > maxTerm {
					maxTerm = currentTerm
				}
			}
			wait.Add(-1)
		}(i)
	}

	half := len(rf.peers) >> 1
	for wait.Load() != 0 {
		if maxTerm > currentTerm || int(votedForMe.Load()) > half {
			fmt.Printf("Election跳出  , 当前term: %d, id: %d, 时间: %d\n", currentTerm, rf.me, getNowTimeMilli()-ss)
			break
		}
		if rf.timeOutCheck() {
			ret = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if currentTerm != rf.currentTerm {
		fmt.Printf("Election失败  , 当前term: %d, id: %d, 发现最新的时期号, 落选为Follower, 时间: %d\n", currentTerm, rf.me, getNowTimeMilli()-ss)
		return ret
	}
	rf.mu.Lock()
	defer rf.updateRecTimeAndUnLock()
	if rf.currentTerm >= maxTerm && int(votedForMe.Load()) > half {
		rf.status = Leader
		fmt.Printf("Election成功  , 当前term: %d, id: %d, voteForme: %d, 当选为Leader, 时间: %d\n", currentTerm, rf.me, votedForMe.Load(), getNowTimeMilli()-ss)
	} else {
		rf.status = Follower
		fmt.Printf("Election失败  , 当前term: %d, id: %d,voteForme: %d, 落选为Follower, 时间: %d\n", currentTerm, rf.me, votedForMe.Load(), getNowTimeMilli()-ss)
	}
	return ret
}

func (rf *Raft) applied() {
	for {
		for rf.status != Leader && rf.lastApplied < rf.commitIndex {
			/**
			有可能会执行命令失败，但目前只会成功 所以不考虑失败的情况 (可能是 1.重试一定次数 2.直接跳过)
			*/
			rf.lastApplied++
		}

		ms := 15 + rand.Int63()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) logReplication(i int) {
	for {

		if rf.status != Leader {
			ms := 250 + rand.Int63()%800
			time.Sleep(time.Duration(ms) * time.Millisecond)
			continue
		}

		nextIndex := rf.nextIndex[i]
		lastIndex := len(rf.fmts) - 1

		if nextIndex <= lastIndex {

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  nextIndex - 1,
				PreLogTerm:   rf.fmts[nextIndex-1].Term,
				Entries:      rf.fmts[nextIndex:],
				LeaderCommit: rf.commitIndex,
			}

			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}

			ok := rf.sendAppendEntries(i, args, reply)

			if ok && rf.status == Leader {
				/* 因为这里只有对应i修改对应数组元素，所以不用担心并发安全 */
				if reply.Success {
					rf.nextIndex[i] = len(rf.fmts)
				} else {
					rf.nextIndex[i] = max(1, rf.nextIndex[i]-1)
				}
			}
		}

		ms := 17 + rand.Int63()%50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := len(rf.fmts) - 1
	lastfmtTerm := rf.fmts[lastIndex].Term
	if args.Term > rf.currentTerm && args.LastfmtTerm >= lastfmtTerm && args.LastfmtIndex >= lastIndex {
		fmt.Printf("RequestVote成功, 当前term: %d, args.term: %d, id: %d, votedFor: %d, 当选Follower, 时间: %d\n",
			rf.currentTerm, args.Term, rf.me, args.CandidateId, getNowTimeMilli()-ss)
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.updateRecTime()
		return
	}
	fmt.Printf("RequestVote失败, 当前term: %d, args.term: %d, id: %d, votedFor: %d, 时间: %d\n",
		rf.currentTerm, args.Term, rf.me, args.CandidateId, getNowTimeMilli()-ss)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isRfTermGtEq := args.Term >= rf.currentTerm
	reply.Term = rf.currentTerm
	reply.Success = false

	if !isRfTermGtEq {
		//	如果当前Term比Leader的大，证明此非Leader，等待选举超时
		fmt.Printf("AppendEntries失败, currentTerm: %d, args.term: %d, id: %d, leader: %d, args.preIndex: %d, commitIdx: %d, len(rf.fmts): %d, "+
			"Leader的Term不是最新\n",
			rf.currentTerm, args.Term, rf.me, args.LeaderId, args.PreLogIndex, rf.commitIndex, len(rf.fmts))
		return
	}

	lastIndex := len(rf.fmts) - 1
	isfmtIndexGt := args.PreLogIndex > lastIndex

	if isfmtIndexGt {
		//	如果Leader的Index比当前lastIndex要大，则需要往前面进行同步，避免缺少数据
		fmt.Printf("AppendEntries失败, currentTerm: %d, args.term: %d, id: %d, leader: %d, args.preIndex: %d, commitIdx: %d, len(rf.fmts): %d, "+
			"日志超前,日志同步需要递减\n",
			rf.currentTerm, args.Term, rf.me, args.LeaderId, args.PreLogIndex, rf.commitIndex, len(rf.fmts))
		rf.updateRecTime()
		return
	}

	isfmtTermEq := args.PreLogTerm == (rf.fmts[args.PreLogIndex]).Term

	if !isfmtTermEq {
		//	日志不一致，则需要往前进行查达成一致的最大的日志索引，避免数据错乱
		fmt.Printf("AppendEntries失败, currentTerm: %d, args.term: %d, id: %d, leader: %d, args.preIndex: %d, commitIdx: %d, len(rf.fmts): %d, "+
			"日志不一致,往前查询一致的最大日志索引\n",
			rf.currentTerm, args.Term, rf.me, args.LeaderId, args.PreLogIndex, rf.commitIndex, len(rf.fmts))
		rf.updateRecTime()
		return
	}

	rf.updateRecTime()
	rf.currentTerm = args.Term
	rf.status = Follower

	if args.PreLogIndex < lastIndex {
		// 删除
		rf.fmts = rf.fmts[:args.PreLogIndex+1]
	}

	if args.Entries != nil {
		// 追加
		rf.fmts = append(rf.fmts, args.Entries...)
	}

	i := rf.commitIndex + 1
	rf.commitIndex = min(len(rf.fmts)-1, args.LeaderCommit)

	rf.sendEntry2Chan(i)

	reply.Term = rf.currentTerm
	reply.Success = true
	fmt.Printf("AppendEntries成功, currentTerm: %d, args.term: %d, id: %d, leader: %d, args.preIndex: %d, commitIdx: %d, len(rf.fmts): %d\n",
		rf.currentTerm, args.Term, rf.me, args.LeaderId, args.PreLogIndex, rf.commitIndex, len(rf.fmts))

}

func (rf *Raft) updateCommitIdx() {
	i := rf.commitIndex + 1
	cni := make([]int, len(rf.nextIndex))
	copy(cni, rf.nextIndex)

	k := len(cni) / 2
	sort.Ints(cni)
	commitIndex := cni[k] - 1

	rf.commitIndex = max(rf.commitIndex, commitIndex)

	rf.sendEntry2Chan(i)
}

func (rf *Raft) sendEntry2Chan(startIndex int) {
	i := startIndex
	for i <= rf.commitIndex {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.fmts[i].Command,
			CommandIndex: i,
		}
		*rf.applyCh <- msg
		fmt.Printf("已发送 %v \n", msg)
		i++
	}
}

func (rf *Raft) updateRecTimeAndUnLock() {
	rf.receiveTime = getNowTimeMilli()
	rf.mu.Unlock()
}

func (rf *Raft) updateRecTime() {
	rf.receiveTime = getNowTimeMilli()
}

func (rf *Raft) timeOutCheck() bool {
	// 1.0s
	ms := time.Second.Milliseconds()
	return getNowTimeMilli()-rf.receiveTime >= ms
}

func getNowTimeMilli() int64 {
	return time.Now().UnixMilli()
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = &applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	if rf.me == 0 {
		rf.receiveTime = 0
	} else {
		rf.receiveTime = getNowTimeMilli()
	}
	rf.fmts = make([]LogEntry, 1)
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 1
		rf.nextIndex[i] = 1
	}
	rf.votedFor = 0
	rf.currentTerm = 0
	rf.mu = sync.Mutex{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	//go rf.applied()
	for i := range rf.peers {
		if i != me {
			go rf.logReplication(i)
		}
	}
	return rf
}
