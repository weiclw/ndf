
class Sink:
    def __init__(self):
        self.is_final_sink = False
        self.output_list = []
        self.sum_list = []
        self.avg_list = []
        self.max_list = []
        self.min_list = []
        self.count_list = []
        self.group_list = []
        self.order_asc_list = []
        self.order_desc_list = []
        self.results = []

    def check(self):
        # check output columns
        out_col_list = [
            self.output_list,
            self.sum_list,
            self.avg_list,
            self.max_list,
            self.min_list,
            self.count_list,
        ]

        total_cols = 0
        for l in out_col_list:
            if len(l) > 0:
                total_cols = total_cols + 1

        if total_cols == 0:
            raise Exception('no output column')

        # Some forbidden cases 
        if len(self.group_list) > 0:
            if len(self.output_list) > 0:
                raise Exception('group and free columns are not compatible')
            
        if len(self.order_asc_list) > 0 and len(self.order_desc_list) > 0:
            raise Exception('cannot have both asc and desc orders')

        return self
    
    def eval(self, line):
        self.results.append(line)
        return self
    
    def sync(self):
        order_list, asc_or_desc = None, None
        if len(self.order_asc_list) > 0:
            asc_or_desc = True
            order_list = self.order_asc_list
        elif len(self.order_desc_list) > 0:
            asc_or_desc = False
            order_list = self.order_desc_list

        if order_list is not None:
            self._order_reduce(asc_or_desc, order_list)

        if len(self.group_list) > 0:
            self._group_reduce(self.group_list)

        return self.results
    
    def _order_reduce(self, asc_or_desc, order_list):
        desc_or_asc = not asc_or_desc
        order_idx_list = [c.get_index() for c in order_list]
        self.results.sort(reverse=desc_or_asc, key=lambda x: [x[i] for i in order_idx_list])
    
    def _group_reduce(self, group_list):
        group_idx_list = [c.get_index() for c in group_list]
        # {"func": [idx1, idx2, ...]}
        output_col_idx_dict = self._get_output_col_index_dict()
        # {[col1, col2, ...] : {[col, "func"] : val}}
        group_dict = dict()

        for line in self.results:
            self._group_output_accumulate(line, group_idx_list, output_col_idx_dict, group_dict)

        # Update and consolidate the results, according to previous orders.
        for line in self.results:
            key = [line[i] for i in group_idx_list]
            outputs = []
            if key in group_dict:
                # {[col, "func"] : val}
                func_dict = group_dict[key]
                # [[col, "func", val], ...]
                func_pair_list = [[k[0], k[1], v] for k,v in func_dict.items()]
                func_pair_list.sort()
                # Construct new output: [aggregated cols,..., group_cols,...,]
                new_line = [v[2] for v in func_pair_list]
                for k in key:
                    new_line.append(k)
                outputs.append(new_line)
                # Remove the key entry from group_dict
                group_dict.pop(key)
                if len(group_dict) == 0:
                    break

        self.results = outputs

    def _group_output_accumulate(self, line, group_idx_list, output_col_idx_dict, group_dict):
        group_key = [line[i] for i in group_idx_list]
        # {[col, "func"] : val}
        group_item_dict = group_dict.setdefault(group_key, dict())

        for func, clist in output_col_idx_dict.items():
            for i in clist:
                func_key = [i, func]
                old_val = group_item_dict.setdefault(func_key, 0)
                if func == 'sum':
                    group_item_dict[func_key] = old_val + line[i]
                elif func == 'avg':
                    group_item_dict[func_key] = float(old_val + line[i]) / 2
                elif func == 'min':
                    group_item_dict[func_key] = line[i] if line[i] < old_val else old_val
                elif func == 'max':
                    group_item_dict[func_key] = line[i] if line[i] > old_val else old_val
                elif func == 'count':
                    group_item_dict[func_key] = old_val + 1
                else:
                    raise Exception(f'func {func} is unknown')

    # Return a list of offsets in line for each functional column
    def _get_output_col_index_dict(self):
        col_list = {
            'sum': self.sum_list,
            'avg': self.avg_list,
            'min': self.min_list,
            'max': self.max_list,
            'count': self.count_list,
        }

        return {k: [c.get_index() for c in l] for k, l in col_list.items()}
    
    def make_final_sink(self):
        self.is_final_sink = True
        return self
    
    def is_final_sink(self):
        return self.is_final_sink

    def output_cols(self, output_list):
        self.output_list = output_list
        return self
    
    def sum_cols(self, sum_list):
        self.sum_list = sum_list
        return self
    
    def avg_cols(self, avg_list):
        self.avg_list = avg_list
        return self
    
    def max_cols(self, max_list):
        self.max_list = max_list
        return self
    
    def min_cols(self, min_list):
        self.min_list = min_list
        return self
    
    def count_cols(self, count_list):
        self.count_list = count_list
        return self
