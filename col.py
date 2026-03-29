
class Col:
    def __init__(self, table, name, index):
        self.table = table
        self.name = name
        self.index = index

    def __repr__(self):
        return self.name
    
    def get_index(self):
        return self.index
    
    def register_sink(self, results):
        self.table.register_sink(results)
        return self