import table
import io

TEXT = """\"id\",\"value\",\"weight\"
1001,20,0.03
1002,23,0.12
1003,21,0.07
"""

def test():
    fh = io.StringIO(TEXT)
    tbl = table.Table(fh=fh)
    print(f'{tbl.schema}')
    print(f'{tbl.results}')

test()