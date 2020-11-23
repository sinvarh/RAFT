from multiprocessing import Process
import time
import zerorpc
import math

import eventlet


def request_rpc(rpc_func):
    def wrapper(self, term, *args):
        rpc_func(self, term, *args)
        if term > self.current_term:
            self.current_term = term
            self.state = self.follower_state

    return wrapper


class State:
    # 定义state基类
    def starts_up(self):
        pass

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
    # 下面3种情况会变成follower
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine
        # 设置超时为30s
        self.timeout = 30

    def waite_rpc(self):
        while (True):
            if self.raft_machine.state == self.raft_machine.follower_state:
                is_timeout = (int(time.time()) - self.raft_machine.last_rpc_time) > self.timeout
                if (is_timeout):
                    # 中断心跳等待
                    self.raft_machine.state = self.raft_machine.candidate_state
                    self.raft_machine.state.timeout_2_start_election()
                    break

    def starts_up(self):
        # 接受rpc请求,启动时都会接受rpc请求
        # 启动超时计算
        p = Process(target=self.waite_rpc(), args=(self,))
        p.start()
        p.join()

    def discovers_current_leader_or_new_term(self):
        self.waite_rpc()
        pass

    def discovers_server_with_higher_term(self):
        self.waite_rpc()
        pass


class CandidateState(State):
    # 下面2种情况会变成follower
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine

    def get_vote_result(self):
        if self.raft_machine.state == self.raft_machine.candidate_state:
            self.raft_machine.current_term += 1
            majority = math.ceil(len(self.raft_machine.machines_url) / 2.0)
            vote_num = 0

            c = zerorpc.Client()
            # 设置超时时间为10s
            with eventlet.Timeout(10):
                for url in self.raft_machine.machines_url:
                    # 如果是自己，不用投票
                    if url is self.raft_machine.url:
                        vote_num += 1
                    else:
                        c.connect(url)
                        vote_res = c.request_vote_rpc(self.raft_machine.current_term, self.raft_machine.id,
                                                      self.raft_machine.commit_index,
                                                      self.raft_machine.log[self.raft_machine.commit_index][0])
                        if (vote_res is True):
                            vote_num += 1
                    if vote_num >= majority:
                        self.raft_machine.state = self.raft_machine.leader_state
                    self.raft_machine.receive_votes_from_majority_servers()

    def timeout_2_start_election(self):
        try:
            self.get_vote_result();
        except Exception as t:
            # 超时的话再选一次
            self.raft_machine.state.timeout_2_new_election()

    def timeout_2_new_election(self):
        # 设置超时时间为10s
        try:
            self.get_vote_result();
        except Exception as t:
            # 超时的话一直选，直达状态改变
            self.raft_machine.state.timeout_2_new_election()
            pass


class LeaderState(State):
    # 下面2种情况会变成leader
    def __init__(self, raft_machine):
        self.raft_machine = raft_machine

    # 有心跳作用
    def send_rpc(self):
        c = zerorpc.Client()
        for url in self.raft_machine.machines_url:
            # 如果不是自己，发送心跳
            if url is not self.raft_machine.url:
                c.connet(url)
                # follower_index = self.raft_machine.commit_index - 1

                if (len(self.raft_machine) - 1) >= self.raft_machine.next_index[url]:

                    append_res = c.append_entries_rpc(self.raft_machine.current_term, self.raft_machine.id,
                                                      self.raft_machine.next_index[url],
                                                      self.raft_machine.log[self.raft_machine.next_index[url]][0],
                                                      [self.raft_machine.log[self.raft_machine.next_index[url]:]],
                                                      self.raft_machine.commit_index)

                    if (append_res is False):
                        # 如果因为日志不一致而失败，减少 nextIndex 重试
                        # todo 这里没有判断是否因日志不一致导致的
                        self.raft_machine.next_index[url] -= 1
                    else:
                        # 前面把所有日志都发过去了
                        self.raft_machine.next_index[url] += len(self.raft_machine.log)
                        self.raft_machine.match_index[url] = self.raft_machine.next_index[url] - 1
        match_next_map = {}
        for match_index in self.raft_machine.match_index:
            # 选出majority
            index = self.raft_machine.match_index[match_index]
            if index > self.raft_machine.commit_index:
                if index not in match_next_map:
                    match_next_map[index] = 1
                else:
                    match_next_map[index] += 1
        for res in match_next_map:
            # 3台机子，majority=2
            if match_next_map[res] > 2 and self.raft_machine.log[res][0] == self.raft_machine.current_term:
                self.raft_machine.commit_index = res

    def receive_votes_from_majority_servers(self):
        next_index = 0
        if len(self.raft_machine.log) > 0:
            next_index = len(self.raft_machine.log)
        self.raft_machine.next_index = {"tcp://127.0.0.1:8889": next_index, "tcp://127.0.0.1:8890": next_index,
                                        "tcp://127.0.0.1:8891": next_index}
        self.raft_machine.match_index = {"tcp://127.0.0.1:8889": 0, "tcp://127.0.0.1:8890": 0,
                                         "tcp://127.0.0.1:8891": 0}

        while (True):
            if self.raft_machine.state == self.raft_machine.leader_state:
                self.send_rpc()


class Raft:
    def __init__(self, id):

        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = []
        self.match_index = []

        # 状态
        self.follower_state = FollowerState(self)
        self.candidate_state = CandidateState(self)
        self.leader_state = LeaderState(self)

        self.machines_url = ["tcp://127.0.0.1:8888", "tcp://127.0.0.1:8889", "tcp://127.0.0.1:8890",
                             "tcp://tcp://127.0.0.1:8891"]
        self.url = "tcp://127.0.0.1:8888"

        # 初始状态
        if self.current_term == 0:
            self.state = self.follower_state
            self.state.starts_up()

        self.leader_id = None
        self.id = id
        # 自己加的状态
        self.last_rpc_time = int(time.time())

    @request_rpc
    def append_entries_rpc(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        # 更新心跳
        if (term == self.current_term):
            self.last_rpc_time = int(time.time())

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

        # 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者
        if term > self.current_term and self.state == self.leader_state:
            self.current_term = term
            self.state = self.follower_state
            self.state.discovers_server_with_higher_term()

        # 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
        if self.state == self.candidate_state and term > self.leader_id != leader_id:
            self.state = self.follower_state
            self.state.discovers_current_leader_or_new_term()

        return True

    @request_rpc
    def request_vote_rpc(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.current_term:
            return False
        #
        if (self.voted_for == None or self.voted_for == candidate_id):
            if (last_log_index >= self.commit_index and last_log_term >= self.log[self.commit_index][0]):
                # 给某个候选人投了票，就自己变成候选人
                self.state = self.candidate_state
                return True
        return False


# 如果commitIndex > lastApplied，那么就 lastApplied 加一，并把log[lastApplied]应用到状态机中
# 有个后台进程把log[lastApplied]应用到状态机中,这里简单把记录改下，实际上应该像mysql binlog一样
def status_machine_apply(raft):
    while (True):
        if raft.commit_index > raft.last_applied:
            raft.last_applied += 1


if __name__ == '__main__':
    p = Process(target=f, args=('bob',))
    p.start()

    print("end")
    p.join()
