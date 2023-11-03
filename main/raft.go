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
	"math/rand"
	"myraft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Role string
type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	FOLLOWER  Role = "follower"
	CANDIDATE Role = "candidate"
	LEADER    Role = "leader"
)
const (
	LeaderHeartBeatTimeout time.Duration = 100 * time.Millisecond
	HeartBeatTimeoutBase   time.Duration = 300 * time.Millisecond
	HeartBeatTimeRand      int64         = 200
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

	curTerm int
	voteFor int
	logs    []*LogEntry

	role      Role
	heartbeat chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.curTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateID   int
}

type RequestVoteReply struct {
	// Your data here (2A).
	ReplyTerm   int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curTerm >= args.CandidateTerm {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, refuse", rf.curTerm, rf.me, args.CandidateID, args.CandidateTerm)
		reply.ReplyTerm = rf.curTerm
		reply.VoteGranted = false
	} else {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, agree, change term", rf.curTerm, rf.me, args.CandidateID, args.CandidateTerm)

		reply.ReplyTerm = rf.curTerm
		reply.VoteGranted = true

		rf.voteFor = args.CandidateID
		rf.curTerm = args.CandidateTerm
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	voteCount := 0
	replyCount := 0
	voteResCh := make(chan bool)
	selectResCh := make(chan bool)

	rf.mu.Lock()
	rf.curTerm++
	rf.voteFor = rf.me
	voteCount++
	term := rf.curTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			getVote := rf.getVote(server, term)
			voteResCh <- getVote
		}(i)
	}

	//waiting for each vote result
	go func() {
		for {
			res := <-voteResCh

			replyCount++
			if res {
				voteCount++
			}
			rf.mu.Lock()
			if rf.role != CANDIDATE {
				selectResCh <- false
				rf.mu.Unlock()
				return
			}
			if voteCount >= len(rf.peers)/2+1 {
				selectResCh <- true
				return
			} else if replyCount >= len(rf.peers) {
				selectResCh <- false
				return
			}
		}
	}()

	result := <-selectResCh
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if result {
		rf.role = LEADER
		go rf.sendHeartBeat()
	} else {
		rf.role = FOLLOWER
	}

}
func (rf *Raft) getVote(server int, term int) bool {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	rf.mu.Lock()
	args.CandidateID = term
	args.CandidateTerm = rf.curTerm
	rf.mu.Unlock()

	DPrintf("[%v] TERM-<%v> send vote request to [%v] in ", rf.me, server, term)
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.ReplyTerm > rf.curTerm {
		rf.role = FOLLOWER
		rf.curTerm = reply.ReplyTerm
		return false
	}
	return reply.VoteGranted
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	ReplyTerm int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeat <- args.LeaderID
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.role = FOLLOWER
	}
	reply.ReplyTerm = rf.curTerm
	reply.Success = true
}
func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LEADER {
			DPrintf("%v [%v] attempts to send heartbeat", rf.role, rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.prepareHeartBeat(i)
		}

		DPrintf("[%v] send heartbeat to all follwers", rf.me)
		time.Sleep(LeaderHeartBeatTimeout)
	}
}
func (rf *Raft) prepareHeartBeat(server int) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	args.LeaderID = rf.me
	args.Term = rf.curTerm
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("[%v] receive bad heartbeat response from [%v]", rf.me, server)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) waitingLeader() {
	for !rf.killed() {
		timeout := HeartBeatTimeoutBase +
			time.Duration(rand.Int63n(HeartBeatTimeRand))*time.Millisecond
		timer := time.NewTimer(timeout)
		select {
		case <-rf.heartbeat:
			timer.Stop()
		case <-timer.C:
			rf.mu.Lock()
			if rf.role == FOLLOWER {
				DPrintf("[%v] heartbeat timeout, start election", rf.me)
				go rf.StartElection()
			}
			rf.mu.Unlock()
		}
	}
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
	rf.voteFor = -1
	rf.role = FOLLOWER
	rf.heartbeat = make(chan int)

	go rf.waitingLeader()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
