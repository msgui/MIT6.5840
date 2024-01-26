package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

	logs []LogEntry

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
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
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
var ss int64 = getNowTimeMilli()

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  0,
				PreLogTerm:   0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					reply := &AppendEntriesReply{
						Term: -1,
					}
					if rf.status != Leader {
						return
					}
					rf.sendAppendEntries(i, args, reply)
					ok := reply.Term > 0
					if ok && reply.Success == false {
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

					}
				}(i)
			}
		}
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func getNowTimeMilli() int64 {
	return time.Now().UnixMilli()
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		if args.Term > rf.currentTerm {
			fmt.Printf("RequestVote成功, 当前term: %d, args.term: %d, id: %d, votedFor: %d, 当选Follower, 时间: %d\n",
				rf.currentTerm, args.Term, rf.me, args.CandidateId, getNowTimeMilli()-ss)
			rf.status = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.updateRecTimeAndUnLock()
			return
		}
		rf.mu.Unlock()
	}
	fmt.Printf("RequestVote失败, 当前term: %d, args.term: %d, id: %d, votedFor: %d, 时间: %d\n",
		rf.currentTerm, args.Term, rf.me, args.CandidateId, getNowTimeMilli()-ss)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{}
	if args.Entries == nil {
		reply.Success = args.Term >= rf.currentTerm
		rf.mu.Lock()
		if reply.Success {
			fmt.Printf("AppendEntries成功, currentTerm: %d, args.term: %d, id: %d, leader: %d \n",
				rf.currentTerm, args.Term, rf.me, args.LeaderId)
			rf.status = Follower
			rf.currentTerm = args.Term
			rf.updateRecTime()
		} else {
			fmt.Printf("AppendEntries失败, currentTerm: %d, args.term: %d, id: %d, leader: %d \n",
				rf.currentTerm, args.Term, rf.me, args.LeaderId)
			reply.Term = rf.currentTerm
		}
		rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	if rf.me == 0 {
		rf.receiveTime = 0
	} else {
		rf.receiveTime = getNowTimeMilli()
	}
	rf.votedFor = 0
	rf.currentTerm = 0
	rf.mu = sync.Mutex{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	return rf
}
