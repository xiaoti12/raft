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
	StateApplyInterval     time.Duration = 200 * time.Millisecond
	LeaderHeartBeatTimeout time.Duration = 100 * time.Millisecond
	HeartBeatTimeoutBase   time.Duration = 300 * time.Millisecond
	HeartBeatTimeRand      int64         = 200 // * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//持久性状态
	curTerm int // start by 0
	voteFor int
	logs    []LogEntry // index start by 1

	//易失性状态
	commitIndex int // start by 0, 0 => no log has been committed
	lastApplied int // start by 0

	nextIndex  []int
	matchIndex []int // each index start by 0

	role      Role
	leaderID  int
	heartbeat chan int
	stopElect chan int

	//for test
	applyCh chan ApplyMsg
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

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteTerm    int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	mylastIndex := rf.getLastLogIndex()
	mylastTerm := 0
	if mylastIndex > 0 {
		mylastTerm = rf.logs[mylastIndex].Term
	}

	reply.VoteTerm = rf.curTerm
	if rf.curTerm >= args.CandidateTerm {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, refuse: candidate term is not higher", rf.me, rf.curTerm, args.CandidateID, args.CandidateTerm)
		reply.VoteGranted = false
	} else if mylastTerm > args.LastLogTerm {
		DPrintf("[%v] receive vote request from [%v], refuse: candidate lastLogTerm #%v < #%v", rf.me, args.CandidateID, args.LastLogTerm, mylastTerm)
		reply.VoteGranted = false
	} else if mylastTerm == args.LastLogTerm && mylastIndex > args.LastLogIndex {
		DPrintf("[%v] receive vote request from [%v], refuse: candidate lastLogIndex #%v < #%v", rf.me, args.CandidateID, args.LastLogIndex, mylastIndex)
		reply.VoteGranted = false
	} else {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, agree, change term", rf.me, rf.curTerm, args.CandidateID, args.CandidateTerm)

		rf.heartbeat <- args.CandidateID
		reply.VoteGranted = true

		rf.voteFor = args.CandidateID
		rf.curTerm = args.CandidateTerm
		rf.leaderID = args.CandidateID
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	FollowerTerm int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.curTerm {
		DPrintf("[%v] TERM-<%v> receive lower term heartbeat from %v TERM-<%v>", rf.me, rf.curTerm, args.LeaderID, args.Term)
		reply.FollowerTerm = rf.curTerm
		reply.Success = false
		return
	}
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
	}
	//reset timer
	rf.heartbeat <- args.LeaderID
	//只有heartbeat的term大于等于自己才会检查follower状态
	if rf.role != FOLLOWER {
		BadPrintf("%v [%v] receive heartbeat, change itself to follower", rf.role, rf.me)
		rf.role = FOLLOWER
	}
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.FollowerTerm = rf.curTerm
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex]
	}
	if len(args.Entries) > 0 {
		DPrintf("[%v] TERM-<%v> receive log#%v", rf.me, rf.curTerm, rf.getLastLogIndex()+1)
	}
	rf.logs = append(rf.logs, args.Entries...)
	newCommit := min(args.LeaderCommit, len(rf.logs)-1)
	if newCommit > rf.commitIndex {
		DPrintf("[%v] TERM-<%v> update commit to log#%v", rf.me, rf.curTerm, newCommit)
	}
	rf.commitIndex = newCommit

	reply.FollowerTerm = rf.curTerm
	reply.Success = true
}

func (rf *Raft) waitingHeartBeat() {
	for !rf.killed() {
		select {
		case <-rf.heartbeat:

		case <-time.After(getRandTimeout()):
			rf.mu.Lock()
			if rf.role == FOLLOWER {
				DPrintf("[%v] TERM-<%v> heartbeat timeout, start election", rf.me, rf.curTerm)
				go rf.StartElection()
			} else if rf.role == CANDIDATE {
				BadPrintf("[%v] TERM-<%v> election timeout, restart", rf.me, rf.curTerm)
				rf.stopElect <- rf.curTerm
				go rf.StartElection()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		start := rf.lastApplied + 1
		end := rf.commitIndex
		if start <= end {
			for i := start; i <= end; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i,
				}
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
			DPrintf("[%v] apply to log %v", rf.me, rf.lastApplied)
		}
		rf.mu.Unlock()
		time.Sleep(StateApplyInterval)
	}
}
func (rf *Raft) getLastLogIndex() int {
	//mostly used in locked area
	return len(rf.logs) - 1
}

func getRandTimeout() time.Duration {
	timeout := HeartBeatTimeoutBase +
		time.Duration(rand.Int63n(HeartBeatTimeRand))*time.Millisecond
	return timeout
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
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	index = rf.getLastLogIndex() + 1
	term = rf.curTerm
	isLeader = rf.role == LEADER
	if isLeader {
		newLog := LogEntry{
			Command: command,
			Term:    term,
		}
		LeaderPrintf("[%v] TERM-<%v> receive log in index #%v", rf.me, term, index)
		rf.logs = append(rf.logs, newLog)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

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
	rf.curTerm = 0
	rf.voteFor = -1
	rf.logs = []LogEntry{LogEntry{}}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.role = FOLLOWER
	rf.heartbeat = make(chan int)
	rf.stopElect = make(chan int)

	rf.applyCh = applyCh

	go rf.waitingHeartBeat()
	go rf.applyLogLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
