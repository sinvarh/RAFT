class raft:
    def __init__(self):
        self.current_term
        self.voted_for
        self.log

        self.commit_index
        self.last_applied

        self.next_index
        self.match_index


    def append_rpc(self,term,leader_id ,prev_log_index,prev_log_term,entries,leader_commit):
        if term<self.current_term:
            return False
        if prev_log_index
        return
    def append_entries_rpc(self,term,leader_id ,prev_log_index,prev_log_term,entries,leader_commit):
        return

    def request_vote_rpc(self,term, candidate_id,last_log_index,last_log_term):
        return



