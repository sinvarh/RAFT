import collections


class raft:
    def __init__(self):
        self.current_term = 0
        self.voted_for =None
        self.log = collections.OrderedDict()

        self.commit_index =0
        self.last_applied = 0

        self.next_index = ()
        self.match_index =  ()


    def append_rpc(self,term,leader_id ,prev_log_index,prev_log_term,entries,leader_commit):
        if term<self.current_term:
            return False
        if prev_log_index not in self.log and self.log[prev_log_index][0] is not prev_log_term :
            return False
        if()


        return
    def append_entries_rpc(self,term,leader_id ,prev_log_index,prev_log_term,entries,leader_commit):
        return

    def request_vote_rpc(self,term, candidate_id,last_log_index,last_log_term):
        return



