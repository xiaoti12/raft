package raft

func (rf *Raft) StartElection() {
	voteCount := 0
	replyCount := 0
	voteResCh := make(chan bool)
	electResCh := make(chan bool)

	rf.mu.Lock()
	// update to candidate role
	rf.role = CANDIDATE
	rf.curTerm++
	rf.voteFor = rf.me
	voteCount++
	//prepare args
	term := rf.curTerm
	lastIndex := len(rf.logs)
	lastTerm := 0
	if lastIndex > 0 {
		lastTerm = rf.logs[lastIndex].Term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			getVote := rf.sendRequestVote(server, term, lastIndex, lastTerm)
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
func (rf *Raft) sendRequestVote(server, term, lastIndex, lastTerm int) bool {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	args.CandidateID = rf.me
	args.CandidateTerm = term
	args.lastLogIndex = lastIndex
	args.lastLogTerm = lastTerm

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