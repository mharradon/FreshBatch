import numpy as np
from threading import Thread
from time import sleep

class FreshBatch(object):
  # FreshBatch takes a function retrieving batches of data from a slow source like a database or from disk
  # (with defined __getitem__ and __len__ functions) and randomly provides samples while running a thread to replace in memory batches
  # with random batches. This enables batches to be drawn mostly randomly while preventing data latency from slowing down training.

  def __init__(self,batch_getter,batch_buffer_size,preload_amount):
    self.batch_getter = batch_getter
    self.batch_buffer_size = batch_buffer_size
    self.batch_buffer = [None]*self.batch_buffer_size
    self.preload_amount = preload_amount
    
    self.start_resampler()

  def start_resampler(self):
    p = Thread(target=resample_loop,args=(self.batch_getter,self.batch_buffer))
    p.start()

  def sample(self,k=16):
    self.block_until_preload(self.preload_amount)
    
    # Fill any "None"s in array first
    good_inds = [i for i,x in enumerate(self.batch_buffer) if x is not None]
    get_inds = [good_inds[i] for i in list(np.random.randint(len(good_inds),size=k))]
    return [self.batch_buffer[i] for i in get_inds]
 
  def block_until_preload(self,preload_amount):
    if sum([1 for x in self.batch_buffer if x is not None]) < preload_amount:
      print("Preloading FreshBatch...")
    while sum([1 for x in self.batch_buffer if x is not None]) < preload_amount:
      sleep(0.1)
    return

  def __len__(self):
    return self.batch_buffer_size

  def __getitem__(self,i):
    return batch_buffer[i]

def resample_loop(batch_getter,mem_buffer):
  while True:
    get_ind = np.random.randint(len(batch_getter))
    
    # Fill any "None"s in array first
    none_inds = [i for i,x in enumerate(mem_buffer) if x is None]
    if len(none_inds)>0:
      replace_ind = none_inds[0]
    else:
      replace_ind = np.random.randint(len(mem_buffer))
    
    mem_buffer[replace_ind] = batch_getter[get_ind]
    sleep(0.01)
  
