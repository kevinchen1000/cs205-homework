# turn off bounds checking & wraparound for arrays
#cython: boundscheck=False, wraparound=False

##################################################
# setup and helper code
##################################################


from cython.parallel import parallel, prange
from openmp cimport omp_lock_t, \
    omp_init_lock, omp_destroy_lock, \
    omp_set_lock, omp_unset_lock, omp_get_thread_num
from libc.stdlib cimport malloc, free

import numpy as np
cimport numpy as np


# lock helper functions
cdef void acquire(omp_lock_t *l) nogil:
    omp_set_lock(l)

cdef void release(omp_lock_t *l) nogil:
    omp_unset_lock(l)

# helper function to fetch and initialize several locks
cdef omp_lock_t *get_N_locks(int N) nogil:
    cdef:
        omp_lock_t *locks = <omp_lock_t *> malloc(N * sizeof(omp_lock_t))
        int idx

    if not locks:
        with gil:
            raise MemoryError()
    for idx in range(N):
        omp_init_lock(&(locks[idx]))

    return locks

cdef void free_N_locks(int N, omp_lock_t *locks) nogil:
    cdef int idx

    for idx in range(N):
        omp_destroy_lock(&(locks[idx]))

    free(<void *> locks)

#helper function to lock pairs in order
cdef void lock_pair(int i, int j, omp_lock_t *locks) nogil:
        if i < j:
            acquire(&locks[i])
            acquire(&locks[j])
        elif i>j:
            acquire(&locks[j])
            acquire(&locks[i]) 

#helper function to lock pairs in order
cdef void release_pair(int i, int j, omp_lock_t *locks) nogil:
        if i < j:
            release(&locks[i])
            release(&locks[j])
        elif i>j:
            release(&locks[j])
            release(&locks[i])

##################################################
# Your code below
##################################################

cpdef move_data_serial(np.int32_t[:] counts,
                       np.int32_t[:] src,
                       np.int32_t[:] dest,
                       int repeat):
   cdef:
       int idx, r

   assert src.size == dest.size, "Sizes of src and dest arrays must match"
   with nogil:
       for r in range(repeat):
           for idx in range(src.shape[0]):
               if counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1


cpdef move_data_fine_grained(np.int32_t[:] counts,
                             np.int32_t[:] src,
                             np.int32_t[:] dest,
                             int repeat):
   cdef:
       int idx, r
       omp_lock_t *locks = get_N_locks(counts.shape[0])

   ##########
   # Your code here
   # Use parallel.prange() and a lock for each element of counts to parallelize
   # data movement.  Be sure to avoid deadlock, and double-locking.
   ##########
   '''
   with nogil:
       for r in range(repeat):
           for idx in range(src.shape[0]):
               if counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1

   free_N_locks(counts.shape[0], locks)
   '''

   
   for r in prange(repeat, nogil=True, num_threads=4):
       for idx in range(src.shape[0]):
           if src[idx] != dest[idx]:
               lock_pair(src[idx], dest[idx], locks)
               if counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1
               release_pair(src[idx], dest[idx], locks)
   free_N_locks(counts.shape[0], locks)
  

   '''
   for r in prange(repeat, nogil=True, num_threads=4):
       for idx in range(src.shape[0]):

           # avoid double locking ==> if moving to itself, do nothing
           if src[idx] != dest[idx]:
               #with gil:
               #  print 'prepare locking ... src[idx]=' ,src[idx],'dest[idx]= ', dest[idx], 'idx =', idx
               lock_pair(src[idx], dest[idx], locks)
               #with gil:
               #  print 'locked locks ... src[idx]=' ,src[idx],'dest[idx]= ', dest[idx]
               if  counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1
               release_pair(src[idx], dest[idx], locks)
                       
    free_N_locks(counts.shape[0], locks)
    '''

cpdef move_data_medium_grained(np.int32_t[:] counts,
                               np.int32_t[:] src,
                               np.int32_t[:] dest,
                               int repeat,
                               int N):
   cdef:
       int idx, r
       int num_locks = (counts.shape[0] + N - 1) / N  # ensure enough locks
       omp_lock_t *locks = get_N_locks(num_locks)

   ##########
   # Your code here
   # Use parallel.prange() and a lock for every N adjacent elements of counts
   # to parallelize data movement.  Be sure to avoid deadlock, as well as
   # double-locking.
   ##########
   '''
   with nogil:
       for r in range(repeat):
           for idx in range(src.shape[0]):
               if counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1

   free_N_locks(num_locks, locks)
   '''

   for r in prange(repeat, nogil=True, num_threads=2):
       for idx in range(src.shape[0]):
           if src[idx] != dest[idx]:
               lock_pair(src[idx]/N, dest[idx]/N, locks)
               if counts[src[idx]] > 0:
                   counts[dest[idx]] += 1
                   counts[src[idx]] -= 1
               release_pair(src[idx]/N, dest[idx]/N, locks)
   free_N_locks(counts.shape[0], locks)
