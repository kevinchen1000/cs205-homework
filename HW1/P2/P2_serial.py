#serial implementation to call functions, figure out program flow, etc
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
from P2 import *

def draw_image_serial(image):
  image = np.log(image+1)


  plt.imshow(image,cmap=cm.gray)
  plt.show()

if __name__ == '__main__':
    #define indices and form outer product
    nr=200.0; nc=200.0
    image = np.zeros((nr,nc))
    x_ind = np.arange(0,nr)
    y_ind = np.arange(0,nc)

    for x in x_ind:
      for y in y_ind:
        image[x,y]=mandelbrot(x/500.0-2,y/500.0-2)        


    #plot 
    #draw_image_serial(image)

    #test plt histogram
        
    data = [0, 0, 13876, 53393, 165543, 266888, 53570, 14042, 0, 0, 0, \
            21703, 65497, 95278, 1853143, 1954388, 95833, 65698, 21902, 0, \
           12530, 44580, 81625, 140141, 12188031, 12289026, 140875, 81721, 44751, 12729, \
           30272, 51524, 108174, 4318918, 16849344, 16893985, 4375631, 108507, 51724, 30472,\
            38539, 49220, 1164982, 15667522, 20440000, 20440000, 15720793, 1213364, 49420, 38739, \
            38559, 40890, 123094, 8034422, 18596168, 18576975, 8117946, 123809, 40961, 38759, \
            30334, 40000, 51270, 108278, 224934, 224845, 108684, 51443, 40000, 30534, 12646, 40000, \
            40000, 44877, 57818, 57852, 44943, 40000, 40000, 12846, 0, 21902, 40000, 40000, 40000, \
            40000, 40000, 40000, 22102, 0, 0, 0, 12729, 30472, 38739, 38759, 30534, 12846, 0, 0]

    plt.figure()
    plt.hist(data,bins = 100, color ='blue',histtype='stepfilled')
    plt.show()
