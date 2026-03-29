import sink
import re

class Filter:
    LESS = "less"
    MORE = "more"
    EQ = "eq"
    RE = "re"

    def __init__(self, col = None, name = None, val = None):
        self.col = col
        self.name = name
        self.val = val
        self.next = None
        self.either = None
        self.sink = None

    def get_column(self):
        return f'{self.col}'

    # Logic OR 
    def either(self, or_list):
        self.either = or_list
        return self
    
    def less(self, col, val):
        self.next = Filter(col, self.LESS, val)
        return self.next
    
    def more(self, col, val):
        self.next = Filter(col, self.MORE, val)
        return self.next

    def eq(self, col, val):
        self.next = Filter(col, self.EQ, val)
        return self.next

    def re(self, col, val):
        self.next = Filter(col, self.RE, val)
        return self.next
    
    def sink(self):
        if self.sink is not None:
            raise Exception('already has a sink')
        
        self.sink = sink.Sink()
        self.col.register_sink(self.sink)
        return self.sink
    
    def eval(self, line):
        # First, if this is the virtual node that link to a sequence of filters.
        if self.col is None and self.either is None:
            if self.next is None:
                raise Exception('no filters at all 1')
            return self.next.eval(line)
        # Second, if this is one of the filters in the sequence.
        elif self.either is None:
            if self.col is None:
                raise Exception('no filters at all 2')
            if not self._named_eval(line):
                return False
            elif self.next is None:
                return False
            else:
                return self.next.eval(line)
        # Otherwise, it has to be the list of OR filters.
        else:
            if self.col is not None:
                raise Exception('either node cannot have a column')
            for f in self.either:
                if f.eval(line):
                    return True
            return False
        
    def _named_eval(self, line):
        idx = self.col.get_index()
        if self.name == self.LESS:
            return line[idx] < self.val
        elif self.name == self.MORE:
            return line[idx] == self.val
        elif self.name == self.EQ:
            return line[idx] == self.val
        elif self.name == self.RE:
            return re.search(self.val, line[idx]) is not None
        else:
            raise Exception(f'unknown op {self.name}')