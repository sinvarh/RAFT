
class raft:
    def __init__(self):
        self.current_term = 0
        self.voted_for =None
        self.log = []

        self.commit_index =0
        self.last_applied = 0

        self.next_index = ()
        self.match_index =  ()



    def append_entries_rpc(self,term,leader_id ,prev_log_index,prev_log_term,entries,leader_commit):
        #参考论文5.1，5.3
        if(term<self.current_term):
            return False
        if(len(self.log)!= 0 and self.log[prev_log_index][0] != prev_log_term):
            return False
        if (len(entries) != 0):
            for entry in entries:
                #新条目索引
                index = entry[0]
                #新条目任期
                index_term = entry[1]
                if(self.log[index][0]!=index_term):
                    #把这条数据往后得log都删除
                    del self.log[index:]
                    break
            #正常追加
            for entry in entries:
                # 新条目索引
                index = entry[0]
                # 新条目任期
                index_term = entry[1]
                if(len(self.log)<index+1):
                    self.log.append(entry)

            if(leader_commit>self.commit_index):
                self.commit_index= min(leader_commit,len(self.log)-1)


    def request_vote_rpc(self,term, candidate_id,last_log_index,last_log_term):
        if term<self.current_term:
            return False
        #
        if(self.voted_for==None or self.voted_for == candidate_id):
            if(last_log_index >= self.commit_index and last_log_term>=self.log[self.commit_index][0]):
                return True
        return False

