import collections
import queue


class StillIterating(Exception):
    pass


class IterableQueue(queue.Queue):

    def _load_next_item(self):
        try:
            self.next_item = next(self.iterator)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        except StopIteration:
            self.next_item = None
            self.iterator = None

    def put_iterable(self, iterable):
        with self.mutex:
            if self.iterator is not None:
                raise StillIterating
            self.iterator = iter(iterable)
            self._load_next_item()

    def _init(self, maxsize):
        self.next_item = None
        self.iterator = None
        self.queue = collections.deque()

    def _qsize(self):
        return len(self.queue) + (1 if self.iterator is not None else 0)

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        if len(self.queue) > 0:
            return self.queue.popleft()
        else:
            item = self.next_item
            self._load_next_item()
            return item

