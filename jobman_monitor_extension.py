"""
.. todo::

    WRITEME
"""
import numpy
np = numpy
from pylearn2.train_extensions import TrainExtension
import theano
import theano.tensor as T
from pylearn2.utils import serial
import jobman
import time

class JobmanMonitor(TrainExtension):
    def __init__( self ):
        #self.__dict__.update(locals())
        self.train_obj = None
        self.channel = None
        self.state = None

    def set_train_obj( self, train_obj ):
        self.train_obj = train_obj

    def set_jobman_channel( self, channel ):
        self.channel = channel

    def set_jobman_state( self,state ):
        self.state = state

    def on_monitor(self, model, dataset, algorithm):
        print "jobman on monitor"
        if self.train_obj!=None and self.state!=None:
          print "calling extract_results"
          t = time.time()
          self.state.results = jobman.tools.resolve(self.state.extract_results)(self.train_obj)
          print "Took",time.time()-t,"seconds"
          if self.channel!=None:              
             print "calling channel save"
             t = time.time()
             self.channel.save()
             print "Took", time.time()-t,"seconds"
