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
	HeartBeatTimeRand      int64         = 200 // * time.Millisecond
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
	stopElect chan int
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curTerm >= args.CandidateTerm {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, refuse", rf.me, rf.curTerm, args.CandidateID, args.CandidateTerm)
		reply.ReplyTerm = rf.curTerm
		reply.VoteGranted = false
	} else {
		DPrintf("[%v] TERM-<%v> receive vote request from [%v] TERM-<%v>, agree, change term", rf.me, rf.curTerm, args.CandidateID, args.CandidateTerm)

		rf.heartbeat <- args.CandidateID

		reply.ReplyTerm = rf.curTerm
		reply.VoteGranted = true

		rf.voteFor = args.CandidateID
		rf.curTerm = args.CandidateTerm
	}
}

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

func (rf *Raft) StartElection() {
	voteCount := 0
	replyCount := 0
	voteResCh := make(chan bool)
	electResCh := make(chan bool)

	rf.mu.Lock()
	rf.role = CANDIDATE
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
			select {
			case from := <-rf.heartbeat:
				DPrintf("[%v] receive heartbeat from [%v], quit election", rf.me, from)
				close(electResCh)
				return

			case term := <-rf.stopElect:
				DPrintf("[%v] TERM-<%v> quit election", rf.me, term)
				close(electResCh)
				return

			case voteRes := <-voteResCh:
				replyCount++
				if voteRes {
					voteCount++
				}
				rf.mu.Lock()
				if rf.role != CANDIDATE {
					DPrintf("[%v] loses candidate role, quit election", rf.me)
					close(electResCh)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if voteCount >= len(rf.peers)/2+1 {
					electResCh <- true
					return
				} else if replyCount >= len(rf.peers) {
					electResCh <- false
					return
				}
			}
		}
	}()

	result, ok := <-electResCh
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && result {
		if rf.role == CANDIDATE {
			DPrintf("[%v] win election in TERM-<%v>", rf.me, rf.curTerm)
			rf.role = LEADER
			DPrintf("[%v] TERM-<%v> starts to send heartbeats", rf.me, rf.curTerm)
			go rf.sendHeartBeat()
		} else {
			DPrintf("[%v] win election in TERM-<%v> but role is wrong", rf.me, rf.curTerm)
		}
	}

}
func (rf *Raft) getVote(server int, term int) bool {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	args.CandidateID = rf.me
	args.CandidateTerm = term

	DPrintf("[%v] TERM-<%v> send vote request to [%v]", rf.me, term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.ReplyTerm > rf.curTerm {
		DPrintf("[%v] TERM-<%v> receive higher term from [%v]", rf.me, rf.curTerm, server)
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
	} else if args.Term < rf.curTerm {
		reply.ReplyTerm = rf.curTerm
		reply.Success = false
		return
	}
	//只有heartbeat的term大于等于自己才会检查follower状态
	if rf.role != FOLLOWER {
		DPrintf("%v [%v] change itself to follower", rf.role, rf.me)
		rf.role = FOLLOWER
	}
	reply.ReplyTerm = rf.curTerm
	reply.Success = true
}
func (rf *Raft) sendHeartBeat() {

	for !rf.killed() {
		rf.mu.Lock()
		term := rf.curTerm
		if rf.role != LEADER {
			DPrintf("%v [%v] attempts to send heartbeat", rf.role, rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		//通过通道给自己发送心跳包
		rf.heartbeat <- rf.me
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.prepareHeartBeat(i, term)
		}

		// DPrintf("[%v] TERM-<%v> send heartbeat to all follwers", rf.me, term)
		time.Sleep(LeaderHeartBeatTimeout)
	}
}
func (rf *Raft) prepareHeartBeat(server int, term int) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}

	args.LeaderID = rf.me
	args.Term = term

	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		// DPrintf("[%v] TERM-<%v> receive bad heartbeat response from [%v]", rf.me, term, server)
		// do something
	} else if !reply.Success {
		DPrintf("[%v] TERM-<%v> receive failed response from [%v]", rf.me, term, server)
		//do something with reply
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
				DPrintf("[%v] TERM-<%v> election timeout, restart", rf.me, rf.curTerm)
				rf.stopElect <- rf.curTerm
				go rf.StartElection()
			}
			rf.mu.Unlock()
		}
	}
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
	rf.voteFor = -1
	rf.role = FOLLOWER
	rf.heartbeat = make(chan int)
	rf.stopElect = make(chan int)
	rf.curTerm = 1

	go rf.waitingHeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
