import csv
import col
import filter

class Table:
    def __init__(self, path=None, fh=None, schema=None):
        self.path = path
        self.schema = schema
        self.schema_dict = dict()
        self.results = []
        self.filter = None
        self.sink_list = []

        if path is not None:
            with open(path, newline='') as f:
                self._load(f)
        elif fh is not None:
            self._load(fh)
        else:
            raise Exception('need either a path or a handle')

        # sanity check for schema
        num_cols = len(self.schema)
        for i,x in enumerate(self.results):
            if len(x) != num_cols:
                raise Exception(f'The {i}th row has {len(x)} columns')

        # Create columns for the table.                
        for idx, n in enumerate(self.schema):
            setattr(self, n, col.Col(self, n, idx))
            self.schema_dict[n] = idx

    def _load(self, f):
        reader = csv.reader(f, quoting=csv.QUOTE_NONNUMERIC)
        results = [x for x in reader]

        if self.schema is None:
            self.schema, *rest = results
            results = rest

        self.results = results
        num_cols = len(self.schema)

    def schema(self):
        return self.schema
    
    def filter(self, reset=False):
        if reset == False and self.filter is not None:
            raise Exception('task has already been set')

        self.sink_list = [] 
        self.filter = filter.Filter()
        return self.filter
    
    def register_sink(self, s):
        self.sink_list.append(s)

    def eval(self):
        final_sink = None
        for r in self.sink_list:
            r.check()
            if r.is_final_sink():
                if final_sink is None:
                    final_sink = r
                else:
                    raise Exception('multiple final sink')
                
        if final_sink is None:
            raise Exception('there is no final sink')

        for line in self.results:
            self.filter.eval(line)

        final_sink.sync()

        return self.results