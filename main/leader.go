package raft

import (
	"time"
)

func (rf *Raft) sendHeartBeat() {
	rf.initFollowerIndex()
	for !rf.killed() {
		rf.mu.Lock()
		// term := rf.curTerm
		// logSize := len(rf.logs)
		if rf.role != LEADER {
			BadPrintf("%v [%v] attempts to send heartbeat", rf.role, rf.me)
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
			go rf.sendAppendEntries(i)
		}

		// DPrintf("[%v] TERM-<%v> send heartbeat to all follwers,logs Size: %v", rf.me, term, logSize)
		time.Sleep(LeaderHeartBeatTimeout)
	}
}
func (rf *Raft) initFollowerIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logSize := len(rf.logs)
	for i := range rf.nextIndex {
		//last log index + 1
		rf.nextIndex[i] = logSize
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	args.LeaderID = rf.me
	args.Term = rf.curTerm

	nextIndex := rf.nextIndex[server]
	args.PrevLogIndex = nextIndex - 1
	if nextIndex <= rf.getLastLogIndex() {
		args.Entries = []LogEntry{rf.logs[nextIndex]}
		// LeaderPrintf("[%v] TERM-<%v> prepare to send log#%v %v to [%v]", rf.me, rf.curTerm, nextIndex, args.Entries, server)
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok || len(args.Entries) == 0 {
		if !ok {
			// LeaderPrintf("[%v] TERM-<%v> fail to receive heartbeat back from [%v]: log#%v %v", rf.me, args.Term, server, nextIndex, args.Entries)
		}
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//process with reply
	if reply.Success {
		// LeaderPrintf("[%v] TERM-<%v> commits log#%v", rf.me, args.Term, nextIndex)
		rf.matchIndex[server] = nextIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// LeaderPrintf("[%v] TERM-<%v> increase [%v] nextIndex to #%v", rf.me, args.Term, server, rf.nextIndex[server])
		rf.updateCommit(server)

	} else {
		//不应该用rf.curTerm，因为如果有若干个节点offline又online
		//第一个heartbeat response会更改leader的curTerm，从而影响后续response的判断
		if reply.FollowerTerm > args.Term {
			BadPrintf("[%v] TERM-<%v> receive [%v] TERM-<%v> heartbeat response,change to follower", rf.me, rf.curTerm, server, reply.FollowerTerm)
			rf.curTerm = reply.FollowerTerm
			rf.role = FOLLOWER
		} else {
			LeaderPrintf("[%v] TERM-<%v> decrease [%v] nextIndex to #%v", rf.me, args.Term, server, nextIndex-1)
			rf.nextIndex[server]--
		}
	}
}
func (rf *Raft) updateCommit(server int) {
	// dont need to require mutex lock
	N := rf.matchIndex[server]
	if N <= rf.commitIndex {
		return
	}
	// exclude leader self
	majority := len(rf.peers) / 2
	count := 0
	for _, m := range rf.matchIndex {
		if m >= N {
			count++
		}
	}
	// N come from nextIndex, which is keeped within logSize
	if count >= majority && rf.logs[N].Term == rf.curTerm {
		rf.commitIndex = N
		LeaderPrintf("[%v] TERM-<%v> commits to log#%v", rf.me, rf.curTerm, N)
	}
}
