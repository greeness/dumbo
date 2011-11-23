from dumbo import *
from math import log

@opt("addpath", "yes")
def mapper1(key, value):
    for word in value.split():
        # key[0]: doc file name (path)
        yield (key[0], word), 1
        # result looks like:
        """
        doc name,     word,    # occurrences
        ------------------------------------- 
        ('./input/2', 'topic')       2
        ('./input/2', 'trademark')   2
        ('./input/2', 'under')       4
        ('./input/2', 'us')          2
        ('./input/2', 'use')         2
        ('./input/2', 'view')        2
        ('./input/2', 'vol.')        4
        ('./input/2', 'was')        12
        ('./input/2', 'went')        2
        ('./input/2', 'where')       4
        ('./input/2', 'which')       4
        ('./input/2', 'whose')       2
        ('./input/2', 'with')        2
        ('./input/2', 'work')        2 
        """

@primary
def mapper2a(key, value):
    # document,  occurrences of a word 
    # after sumreduce, this is total number of words in this document
    yield key[0], value

@secondary
def mapper2b(key, value):
    # document, for each word, total occurrences
    yield key[0], (key[1], value)

@primary
def mapper3a(key, value):
    # value[0]: word
    # after sumreduce, this is total number of occurrences of the word
    # across all documents
    yield value[0], 1

@secondary
def mapper3b(key, value):
    # value[0]: word, # of occurrences
    # key:      doc name
    # value[1]: term frequency
    yield value[0], (key, value[1])

class Reducer(JoinReducer):
    def __init__(self):
        self.sum = 0
    def primary(self, key, values):
        self.sum = sum(values)

class Combiner(JoinCombiner):
    def primary(self, key, values):
        yield key, sum(values)

class Reducer1(Reducer):
    def secondary(self, key, values):
        doc = key
        for (word, n) in values:
            # self.sum : total number of words in this doc
            # n: number of occurrences of this word
            # n/self.sum: term frequency of this doc
            yield doc, (word, float(n) / self.sum)
            
            # result looks like
            """
            doc_name       (word,    word frequency )
            ----------------------------------------------
            './input/2'    ('use',   0.00186219739292365)
            './input/2'    ('view',  0.00186219739292365)
            './input/2'    ('vol.',  0.0037243947858473)
            './input/2'    ('was',   0.0111731843575419)
            './input/2'    ('went',  0.00186219739292365)
            './input/2'    ('where', 0.0037243947858473)
            './input/2'    ('which', 0.0037243947858473)
            './input/2'    ('whose', 0.00186219739292365)
            './input/2'    ('with',  0.00186219739292365)
            './input/2'    ('work',  0.00186219739292365)
            './input/2'    ('works', 0.00186219739292365)
            './input/2'    ('years', 0.00186219739292365)
            """

class Reducer2(Reducer):
    def __init__(self):
        Reducer.__init__(self)
        self.doccount = float(self.params["doccount"])
    def secondary(self, key, values):
        # inverse document freqency: 
        #  = # of document / total # of occur of the word in all docs
        idf = log(self.doccount / self.sum)
        word = key
        for (doc, tf) in values:
            yield (word, doc), tf * idf
            # result looks like:
            """
            (word,     doc name)       tf-idf value
            ('years',  './input/1')    0.0
            ('years',  './input/2')    0.0
            ('years:', './input/1')    0.0001237541832815471
            ('you',    './input/1')    0.0002475083665630942
            ('young',  './input/1')    0.0001237541832815471
            ('younger','./input/1')    0.0002475083665630942
            ('your',   './input/1')    0.0002475083665630942
            """
            
def runner(job):
    job.additer(mapper1, sumreducer, combiner=sumreducer)
    
    multimapper = MultiMapper()
    multimapper.add("", mapper2a)
    multimapper.add("", mapper2b)
    job.additer(multimapper, Reducer1, Combiner)
    
    multimapper = MultiMapper()
    multimapper.add("", mapper3a)
    multimapper.add("", mapper3b)
    job.additer(multimapper, Reducer2, Combiner)
    
#dumbo start tfidf.py -input tf_idf_input* -output test -param doccount=2
if __name__ == "__main__":
    main(runner)
