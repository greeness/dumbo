from dumbo import *

""" input data
    5
    10
    4
    1
    50
    1
    5
    24
    89
    11
    100
    """

def read_raw(key, value):
    data = value.strip()
    yield 'ITEM', int(data)
    """output:
    'ITEM'    5
    'ITEM'    10
    'ITEM'    4
    'ITEM'    1
    'ITEM'    50
    'ITEM'    1
    'ITEM'    5
    'ITEM'    24
    'ITEM'    89
    'ITEM'    11
    'ITEM'    100
    """

@primary
def mapper1(key,value):
    yield key, value
    
@secondary
def mapper2(key,value):
    yield key, value

class Reducer(JoinReducer):
    def __init__(self):
        self.sum = 0
    def primary(self, key, values):
        self.sum = sum(values)
        
class Reducer1(Reducer):
    def secondary(self, key, values):
        for i in values:
            yield 'NORM', (i, self.sum, float(i)/self.sum)
        """output:
        'NORM'    (1,   300, 0.0033333333333333335)
        'NORM'    (1,   300, 0.0033333333333333335)
        'NORM'    (10,  300, 0.03333333333333333)
        'NORM'    (100, 300, 0.3333333333333333)
        'NORM'    (11,  300, 0.03666666666666667)
        'NORM'    (24,  300, 0.08)
        'NORM'    (4,   300, 0.013333333333333334)
        'NORM'    (5,   300, 0.016666666666666666)
        'NORM'    (5,   300, 0.016666666666666666)
        'NORM'    (50,  300, 0.16666666666666666)
        'NORM'    (89,  300, 0.2966666666666667)
        """

def runner(job):
    job.additer(read_raw)
    multimapper = MultiMapper()
    multimapper.add("", mapper1)
    multimapper.add("", mapper2)
    job.additer(multimapper, Reducer1)
    
if __name__ == "__main__":
    main(runner)