###########################################################################
#
#   Copyright 2010-2015, A.J. Rubio-Montero (CIEMAT - Sci-track R&D group)         
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
###########################################################################
#
#   Additional license statement: 
#
#    If you use this code to perform any kind of research, report, 
#    documentation, or development you should properly cite GWpilot in your 
#    work with the following reference:
#
#    A.J. Rubio-Montero et al., GWpilot: Enabling multi-level scheduling in 
#    distributed infrastructures with GridWay and pilot Jobs, 
#    Future Generation Computer Systems 45 (2015) 25--52.   
#    http://dx.doi.org/10.1016/j.future.2014.10.003
#
#    and/or with any related paper listed at: 
#         http://rdgroups.ciemat.es/web/sci-track/
#
###########################################################################

import cherrypy
from cherrypy.process import plugins
import logging
import thread
from threading import *
import urllib
import urllib2
import os
import subprocess
from GWpilot_PilotList import PilotList
from GWutils.EMutils.GWJob import *
class PilotManager(object):
    def __init__ (self, pilot_list, logger=None):
        self.logger=logger 
        self.pilot_list=pilot_list
    def _pilot_is_valid_and_have_matched_job(self,pilotIDs,jid=None):
        if not self.pilot_list.is_a_valid_pilot(pilotIDs):
            if self.logger!=None :  self.logger.info("ERROR PILOT IS NOT VALID: pilot_id=%s" % pilotIDs['HOSTNAME'])
            return (False,None,None)
        pilot_id=pilotIDs['HOSTNAME']
        j=self.pilot_list.get_possible_match(pilot_id)
        if j==None: 
            return (True,None,None)
        (rsl, pilot_match)= GWJobWrapper().getmatch(j)
        if pilot_id!=pilot_match :
            return (True,None,None)
        if jid!=None :
            if j.jid != jid:
                return (True,None,None)
        return (True,j, rsl)
    def index(self):
        return "ERROR"
    index.exposed = True
    def ping(self,**hash):
        return hash['HASH']
    ping.exposed = True
    def getJob(self,**pilotIDs):
        pilot_id=pilotIDs['HOSTNAME']
        if self.logger!=None :  self.logger.info("GET JOB by pilot: pilot_id=%s" % pilot_id)
        (isvalid,j,rsl) = self._pilot_is_valid_and_have_matched_job(pilotIDs)
        if not isvalid:
            if self.logger!=None :  self.logger.info("GET JOB FAILED pilot INVALID: pilot_id=%s" % pilotIDs['HOSTNAME'])
            return 'ERROR'            
        res_rsl=''
        if j!=None: 
            if rsl!=None:
                if self.logger!=None :  self.logger.info("RSL found for pilot: rsl=%s" % rsl)
                GWJobWrapper().setmatch(j, (None,pilot_id))
                signal=GWJobWrapper().getsignal(j)
                if signal=='KILL':
                    GWJobWrapper().setstatus(j,'DONE')
                else: 
                    if self.logger!=None :  self.logger.info("JobId got by a pilot: HOSTNAME, job_id  \"%s\" " % " ".join([pilot_id,j.jid]))    
                    res_rsl=rsl
        return res_rsl
    getJob.exposed = True  
    def updateJobState(self,**state):
        pilotIDs={}
        pilotIDs['HOSTNAME']=state['HOSTNAME']
        pilotIDs['PILOT_HASH_NAME']=state['PILOT_HASH_NAME']
        (isvalid,j,rsl) = self._pilot_is_valid_and_have_matched_job(pilotIDs,state['job_id'])
        if j==None:
            if self.logger!=None :  self.logger.info("UPDATE job state FAILED: GW_JOB_ID, state \"%s\" " % " ".join([state['job_id'], state['job_state']]))         
            return 'ERROR'
        GWJobWrapper().setstatus(j, state['job_state'])
        if self.logger!=None :  self.logger.info("Job state updated: GW_JOB_ID, state \"%s\" " % " ".join([state['job_id'], state['job_state']]))
        return 'OK'  
    updateJobState.exposed = True
    def getJobSignals(self,**state):
        pilotIDs={}
        pilotIDs['HOSTNAME']=state['HOSTNAME']
        pilotIDs['PILOT_HASH_NAME']=state['PILOT_HASH_NAME']
        (isvalid,j,rsl) = self._pilot_is_valid_and_have_matched_job(pilotIDs,state['job_id'])
        if j==None:
            if self.logger!=None :  self.logger.info("Job signal FAILED: GW_JOB_ID, signal \"%s\" " % " ".join([state['job_id'], signal]))         
            return 'ERROR'
        aux_signal=GWJobWrapper().getsignal(j)
        signal=''
        if aux_signal!=None :
            signal=aux_signal
            GWJobWrapper().setsignal(j, None)
        self.logger.info("Job Signal got by a pilot: GW_JOB_ID, signal \"%s\" " % " ".join([state['job_id'], signal]))    
        return signal
    getJobSignals.exposed = True
    def notifyPilot(self,**remote_state):
        new_hostname = self.pilot_list.include_new_pilot(remote_state)
        return new_hostname   
    notifyPilot.exposed = True   
    def updatePilotState(self,**remote_state):
        result=self.pilot_list.update_pilot(remote_state)        
        return result
    updatePilotState.exposed = True
class PilotServer(Thread):
    def __init__ (self, pilot_list, number_chpy_threads, logger=None, with_https=True, what_port=24999):
        Thread.__init__(self)
        self.number_chpy_threads=number_chpy_threads
        self.logger=logger
        self.pilotmgr=PilotManager(pilot_list, self.logger)
        self.https=with_https
        self.port=what_port
    def run(self):
        opts = {'server.socket_port': self.port,
                'server.socket_host': '0.0.0.0',
                'server.thread_pool': self.number_chpy_threads,
                'log.screen':False,
                'log.error_file':'/dev/null',
                'checker.on': False
                }
        if self.https :
            environment=os.environ
            user=environment['USER']
            proxy_file=environment['X509_USER_PROXY']
            usercert='/tmp/'+user+'_cert'
            userkey=proxy_file
            cmd='openssl x509 -in ' + proxy_file + ' -out ' + usercert
            subprocess.Popen(cmd, shell = True, stdin=None, stdout=None, stderr=None)
            opts['server.ssl_certificate']=usercert
            opts['server.ssl_private_key']=userkey
        cherrypy.config.update(opts)
        cherrypy.quickstart(self.pilotmgr)
