"""
dedupe.py — remembers recent event_ids so a repeated event is skipped.

Why: Albert's validator delivers every event AT LEAST once. After a crash it can
send the same event twice. If the scorer counted it twice, that card's history
would be wrong ("5 payments in 24h" when there were really 4). So the scorer
checks every event_id here first.

Memory: about 100 bytes per id, so the default 500,000 ids is ~50 MB.
Ids older than that are forgotten (a repeat that late is extremely unlikely).
"""

from collections import deque


class RecentIds:
    def __init__(self, maxlen=500_000):
        self.maxlen = maxlen
        self._set = set()
        self._order = deque()

    def __contains__(self, event_id):
        return event_id in self._set

    def __len__(self):
        return len(self._set)

    def add(self, event_id):
        if event_id in self._set:
            return
        self._set.add(event_id)
        self._order.append(event_id)
        if len(self._order) > self.maxlen:
            self._set.discard(self._order.popleft())
