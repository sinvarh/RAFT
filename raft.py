from multiprocessing import Process


def request_rpc(rpc_func):
    def wrapper(self, term, *args):
        rpc_func(self, term, *args)
        if term > self.current_term:
            self.current_term = term
            self.state = self.follower_state
    return wrapper


class State:
    # 定义state基类
    def timeout_2_start_election(self):
        pass

    def timeout_2_new_election(self):
        pass

    def receive_votes_from_majority_servers(self):
        pass

    def discovers_current_leader_or_new_term(self):
        pass

    def discovers_server_with_higher_term(self):
        pass


class FollowerState(State):
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine


class CandidateState(State):
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine


class LeaderState(State):
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine


class Raft:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = ()
        self.match_index = ()

        # 状态
        self.follower_state = FollowerState(self)
        self.candidate_state = CandidateState(self)
        self.leader_state = LeaderState(self)

        # 初始状态
        if self.current_term == 0:
            self.state = self.follower_state

    @request_rpc
    def append_entries_rpc(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        # 参考论文5.1，5.3
        if (term < self.current_term):
            return False
        if (len(self.log) != 0 and self.log[prev_log_index][0] != prev_log_term):
            return False
        if (len(entries) != 0):
            for entry in entries:
                # 新条目索引
                index = entry[0]
                # 新条目任期
                index_term = entry[1]
                if (self.log[index][0] != index_term):
                    # 把这条数据往后得log都删除
                    del self.log[index:]
                    break
            # 正常追加
            for entry in entries:
                # 新条目索引
                index = entry[0]
                # 新条目任期
                index_term = entry[1]
                if (len(self.log) < index + 1):
                    self.log.append(entry)

            if (leader_commit > self.commit_index):
                self.commit_index = min(leader_commit, len(self.log) - 1)

    @request_rpc
    def request_vote_rpc(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.current_term:
            return False
        #
        if (self.voted_for == None or self.voted_for == candidate_id):
            if (last_log_index >= self.commit_index and last_log_term >= self.log[self.commit_index][0]):
                return True
        return False


# 把log[lastApplied]应用到状态机中
timout = 30


def waite_rpc(raft):


if __name__ == '__main__':
    p = Process(target=f, args=('bob',))
    p.start()

    print("end")
    p.join()
