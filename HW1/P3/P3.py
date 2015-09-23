import pyspark 

def list_anagram(v1, v2):
  return v1+v2;

if __name__ == "__main__":
  sc = pyspark.SparkContext()
  rdd_words = sc.textFile('testWords.txt')

  # sort, put into list, then reduce
  rdd_sequence_word = rdd_words.map(lambda x: (''.join(sorted(x)),[x]))\
                               .reduceByKey(list_anagram)

  print rdd_sequence_word.take(10)

  #count
  rdd_result = rdd_sequence_word.map(lambda x: (x[0],len(x[1]),x[1])).sortBy(lambda x: x[1],False) 

  print rdd_result.take(10)
