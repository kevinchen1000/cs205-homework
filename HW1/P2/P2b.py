from P2 import *
import numpy as np
import pyspark

#define a partition function
def random_partitionFn(x):
  #return a random number in the range from 0 to 99
  max_num = 99+1  #note this does not need to be hard-coded, can be made into a field of an object, the object has a partition function 
  return int(np.random.random()*max_num)


# Your code here
if __name__ == "__main__":
  print "start slow partitioning case: ...\n"
  
  #split into 10 by 10 blocks
  sc = pyspark.SparkContext()
  nr = 2000
  nc = 2000
  x_ind_rdd = sc.parallelize(range(0,nr),1)
  y_ind_rdd = sc.parallelize(np.arange(0,nc,dtype=np.int64),1)

  ind_rdd = x_ind_rdd.cartesian(y_ind_rdd)
  ind_rdd = ind_rdd.partitionBy(100,random_partitionFn)   #more shuffle time (~6s, but very good load balancing)
  #print ind_rdd.take(10)

  #transform rdd
  rdd_image = ind_rdd.map(lambda ind: (ind, mandelbrot( (ind[0]/500.0-2)\
                                                       ,(ind[1]/500.0-2))))

  #compute histogram
  rdd_compute = sum_values_for_partitions(rdd_image)
  data = rdd_compute.collect()
  fig= plt.figure()
  #plt.hist(data,bins=100,color='blue')
  plt.plot(data,color='blue')
  plt.savefig('P2b_hist.png')
  plt.close(fig)

 
  #draw image
  draw_image(rdd_image)
