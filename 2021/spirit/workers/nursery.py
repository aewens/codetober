from sys import exit
from os import fork, getpid, getppid, kill, waitpid
from signal import signal, SIGKILL, SIGINT, SIGUSR1

class Nursery:
    def __init__(self, parent_callback, child_callbacks):
        self._is_parent = None
        self._is_child = None
        self._root = None

        self.set_parent_callback(parent_callback)
        self.set_child_callbacks(child_callbacks)

    def __str__(self):
        return str(self._root)

    def set_parent_callback(self, callback):
        self._parent_callback = callback

    def set_child_callbacks(self, callbacks):
        self._child_callbacks = callbacks

    def handle_parent(self, states, pids):
        # Reap children on Ctrl-C
        signal(SIGINT, lambda: self.reap(pids))

        # Re-spawn the child workers
        signal(SIGUSR1, lambda: self.spawn(states, pids))

        pid = getpid()
        self._is_parent = True
        self._is_child = False
        self._root = self._parent_callback(self, states, pid, pids)

        #self.spawn(states, pids=pids)
        for p in range(len(pids)):
            pid = waitpid(0, 0)
            index = pids[pid[0]]
            print(f"Worker {index+1} of {len(pids)} closed")

    def handle_child(self, state, index):
        pid = getpid()
        ppid = getppid()
        callback = self._child_callbacks[index]

        self._is_child = True
        self._is_parent = False
        self._root = callback(index, state, pid, ppid)

        # Make sure the child process never returns from this function
        exit(0)

    def reap(self, pids):
        for pid in pids:
            kill(pid, SIGKILL)

    def spawn(self, states, pids=list()):
        self.reap(pids)

        pids = dict()
        children = len(states)
        for i in range(children):
            fpid = fork()
            if fpid == 0:
                self.handle_child(states[i], i)

            else:
                pids[fpid] = i

        self.handle_parent(states, pids)
