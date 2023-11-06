package raft

import "time"

func (rf *Raft) sendHeartBeat() {

	for !rf.killed() {
		rf.mu.Lock()
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
			go rf.sendAppendEntries(i)
		}

		// DPrintf("[%v] TERM-<%v> send heartbeat to all follwers", rf.me, term)
		time.Sleep(LeaderHeartBeatTimeout)
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
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.Entries = []LogEntry{rf.logs[nextIndex]}
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		// DPrintf("[%v] TERM-<%v> receive bad heartbeat response from [%v]", rf.me, term, server)
		// do something
	} else if !reply.Success {
		DPrintf("[%v] TERM-<%v> receive failed response from [%v]", rf.me, args.Term, server)
		//do something with reply
	}
}
