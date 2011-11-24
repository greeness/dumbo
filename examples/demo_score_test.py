from dumbo import *

@primary
def mapper1(key,value):
    src, category, values = value.strip().split('\t')
    for vid, score in eval(values):
        yield vid, (score, src, category)
        
@secondary
def mapper2(key,value):
    vid, demo_score = value.strip().split('\t')
    yield vid, float(demo_score)

class Reducer(JoinReducer):
    def primary(self, key, values):
        self.catlist = list(values)

    def secondary(self, key, values):
        demo = values.next()
        for score, src, cat in self.catlist:
            yield (src,cat), (key, score*demo)
        """
        ('youtube', 'sports')    ('video1', 0.0004)
        ('youtube', 'music')     ('video1', 0.004)
        ('youtube', 'music')     ('video2', 0.06)
        ('youtube', 'sports')    ('video2', 0.2)
        ('youtube', 'tech')      ('video2', 0.2)
        ('youtube', 'sports')    ('video3', 0.12)
        ('youtube', 'music')     ('video4', 0.0005)
        ('youtube', 'tech')      ('video4', 0.052)
        """
        
def merge_reducer(key, values):
    yield key, list(values)        
    """
    youtube    music   [('video1', 0.4),  ('video2', 0.3), ('video4', 0.01)]
    youtube    sports  [('video2', 1.0),  ('video3', 0.4), ('video1', 0.04)]
    youtube    tech    [('video4', 1.04), ('video2', 1.00)]
    """
        
def runner(job):
    #job.additer(mapper2)
    multimapper = MultiMapper()
    multimapper.add("score.txt", mapper1)
    multimapper.add("demo.txt", mapper2)
    job.additer(multimapper, Reducer)
    job.additer(identitymapper, merge_reducer)
    
if __name__ == "__main__":
    main(runner)