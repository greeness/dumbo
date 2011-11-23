from dumbo import *

def read_raw(key, value):
    source, vid, number = value.strip().split('\t')
    yield (source, vid), float(number)
    """
    source   vid    metric#
    -------------------------
    youtube    1    5
    youtube    1    10
    vimeo      2    4
    vimeo      3    1
    vimeo      3    50
    vimeo      4    1
    vimeo      5    5
    youtube    6    24
    youtube    6    89
    youtube    7    11
    youtube    8    100
    """
    
@primary
def mapper1(key, value):
    source, vid = key
    yield source, value
    
@secondary
def mapper2(key, value):
    source, vid = key
    yield source, (vid, value)
    
class Combiner(JoinCombiner):
    def primary(self, key, values):
        yield key, sum(values)
        
class Reducer(JoinReducer):
    def __init__(self):
        self.sum = 0
    def primary(self, key, values):
        self.source = key
        self.sum = sum(values)
        
class Reducer1(Reducer):
    def secondary(self, key, values):
        total_in_source = self.sum
        vid = key
        for vid,value in values:
            #yield vid, (value, total_in_source, value/total_in_source)
            yield (vid, self.source), value/total_in_source
            
            """
            (vid,  source)      normalized number in its source
            ----------------------------------------------------
            ('1', 'youtube')    0.06276150627615062
            ('2', 'vimeo')      0.06557377049180328
            ('3', 'vimeo')      0.8360655737704918
            ('4', 'vimeo')      0.01639344262295082
            ('5', 'vimeo')      0.08196721311475409
            ('6', 'youtube')    0.4728033472803347
            ('7', 'youtube')    0.04602510460251046
            ('8', 'youtube')    0.41841004184100417
            """
            
def runner(job):
    job.additer(read_raw)
    multimapper = MultiMapper()
    multimapper.add("", mapper1)
    multimapper.add("", mapper2)
    job.additer(multimapper, Reducer1, Combiner)
    job.additer(identitymapper, sumreducer)

# dumbo start normalize.py -output test -input normalize.txt 
if __name__ == "__main__":
    main(runner)
