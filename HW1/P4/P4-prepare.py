#script to prepare the connectivity list in spark
import numpy as np
import pyspark

def mapper_l1(line):
  hero, issue = line.split('"')[1::2]
  return (hero, issue)

if __name__ == '__main__':
  print 'prepare adj list begins ...\n'

  # read in file
  sc = pyspark.SparkContext()
  rdd_lines = sc.textFile('source.csv')
  print rdd_lines.take(10)

  #mapper level 1
  rdd_list = rdd_lines.map(mapper_l1)
  print rdd_list.take(10)

  
