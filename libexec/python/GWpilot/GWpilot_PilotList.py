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

import os,time,subprocess
from threading import *
from Queue import *
from pilot import PilotState    
from GWutils.EMutils.GWJob import GWJobWrapper
import fcntl, warnings 
def lock_and_open_file(filename,mode):
    f=open(filename,mode)
    try:
        fcntl.flock(f, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except IOError:
        warnings.warn("can't immediately write-lock the file ($!), blocking ...")
        fcntl.flock(f, fcntl.LOCK_EX)     
    return f
def trunc_and_write_locked_file(filedesc,data):
    filedesc.seek(0, 0)
    filedesc.writelines(data)
    filedesc.truncate(filedesc.tell())
class PilotList(object):
    class _RegisteredPilot(PilotState):
        def  __init__ (self, virtual_hostname):
            self.state={}
            self.state['HOSTNAME']=virtual_hostname
            self.state['PILOT_HASH_NAME']=None
            self.state['PILOT_REAL_HOSTNAME']=None
            self.state['PILOT_REAL_RESOURCE']=None
            self.failed_updates=0
            self.isfree=True
            self.gwjob=None
            self.primitive_lock=Lock()
        def free (self):
            self.failed_updates=0
            self.isfree=True
        def use (self, remote_state):
            self.failed_updates=0
            self.isfree=False
            bak_hostname=self.state['HOSTNAME']
            self.updateStateBulk(remote_state)
            self.state['HOSTNAME']=bak_hostname
            return bak_hostname
    class _PilotListManager(Thread):
        def  __init__ (self,pilot_list_cont,im_file,logger=None, priv_func=None, args=(), kwargs={}):
            if priv_func!=None:            
                private_functions={
                                   'include_new_pilot': self.include_new_pilot,
                                   'is_a_valid_pilot' : self.is_a_valid_pilot,
                                   'update_pilot': self.update_pilot,
                                   'clean_pilot_reg_list': self.clean_pilot_reg_list, 
                                   'get_possible_match': self.get_possible_match,
                                   'set_possible_match': self.set_possible_match,
                                   'remove_possible_match': self.remove_possible_match                             
                                   }
                Thread.__init__(self,target=private_functions[priv_func],args=args,kwargs=kwargs)
            else:
                Thread.__init__(self)
            self.logger=logger
            self.im_file=im_file
            self.pilot_reg_list=pilot_list_cont.pilot_reg_list
            self.last_used=pilot_list_cont.last_used
            self.map=pilot_list_cont.map
        def _search_and_include_virtual_hostname_free(self,remote_state):
            last_hostname=self.last_used.state['HOSTNAME']
            found=False
            new_hostname='ERROR'   
            for k, p in self.pilot_reg_list.iteritems() :
                if found==False and k==last_hostname : 
                    found=True
                    continue
                if found==True :
                    p.primitive_lock.acquire()    
                    if p.isfree==True :
                        self.last_used=p
                        new_hostname=p.use(remote_state)
                        self.map[p.state['PILOT_HASH_NAME']]=p
                        p.primitive_lock.release()
                        break
                    p.primitive_lock.release()
            if new_hostname=='ERROR': 
                for p in self.pilot_reg_list.values() :
                    p.primitive_lock.acquire()
                    if p.isfree==True :
                        self.last_used=p
                        new_hostname=p.use(remote_state)
                        self.map[p.state['PILOT_HASH_NAME']]=p
                        p.primitive_lock.release()
                        break
                    p.primitive_lock.release()
                if self.logger!=None :  self.logger.info("Assigning pilot ID previously used to \"%s\" " % " ".join([remote_state['PILOT_REAL_HOSTNAME'],new_hostname]))
            return new_hostname    
        def _reuse_previous_pilot_free(self,remote_state):                             
            hash_name=remote_state['PILOT_HASH_NAME']
            if hash_name in self.map.keys():
                return self.map[hash_name].state['HOSTNAME']
            new_hostname='ERROR' 
            pilot_real_hostname=remote_state['PILOT_REAL_HOSTNAME']
            pilot_real_resource=remote_state['PILOT_REAL_RESOURCE']
            for p in self.pilot_reg_list.values():
                p.primitive_lock.acquire()
                if p.isfree==True and p.state['PILOT_REAL_HOSTNAME'] == pilot_real_hostname and p.state['PILOT_REAL_RESOURCE']==pilot_real_resource :
                    new_hostname=p.use(remote_state)
                    self.map[p.state['PILOT_HASH_NAME']]=p
                    p.primitive_lock.release()
                    break
                p.primitive_lock.release()
            return new_hostname
        def include_new_pilot(self, remote_state, q=None):
            new_hostname=self._reuse_previous_pilot_free(remote_state)  
            if new_hostname=='ERROR' :            
                new_hostname=self._search_and_include_virtual_hostname_free(remote_state)        
                if self.logger!=None :  self.logger.info("Pilot notified and included: PILOT_REAL_HOSTNAME as HOSTNAME \"%s\" " % " ".join([remote_state['PILOT_REAL_HOSTNAME'],new_hostname]))
            else:
                if self.logger!=None :  self.logger.info("Pilot RE-notified: PILOT_REAL_HOSTNAME as HOSTNAME \"%s\" " % " ".join([remote_state['PILOT_REAL_HOSTNAME'],new_hostname]))
            if q!=None : q.put(new_hostname)
            return new_hostname
        def is_a_valid_pilot(self, pilotIDs, q=None):
            pilot_id=pilotIDs['HOSTNAME']
            pilot_hash_id=pilotIDs['PILOT_HASH_NAME']
            result=False
            if pilot_id in self.pilot_reg_list.keys() :
                p=self.pilot_reg_list[pilot_id]
                p.primitive_lock.acquire()
                if p.state['PILOT_HASH_NAME'] == pilot_hash_id:
                    result=True
                p.primitive_lock.release()
            if q!=None : q.put(result)    
            return result
        def update_pilot(self, remote_state, q=None):
            res='ERROR'
            pilot_id=remote_state['HOSTNAME']
            pilot_hash_id=remote_state['PILOT_HASH_NAME']
            if pilot_id in self.pilot_reg_list.keys() :
                p=self.pilot_reg_list[pilot_id]
                p.primitive_lock.acquire()
                if p.state['PILOT_HASH_NAME'] == pilot_hash_id :
                    p.use(remote_state)
                    self.map[p.state['PILOT_HASH_NAME']]=p
                    p.primitive_lock.release()
                    if self.logger!=None :  self.logger.info("Pilot updated: PILOT_REAL_HOSTNAME as HOSTNAME \"%s\" " % " ".join([remote_state['PILOT_REAL_HOSTNAME'], pilot_id]))
                    res='OK'
                else:
                    p.primitive_lock.release() 
                    if self.logger!=None :  self.logger.info("Update failed for Pilot with PILOT_HASH_NAME as HOSTNAME \"%s\" " % " ".join([pilot_hash_id, pilot_id]))
                    res='ERROR'
            if q!=None : q.put(res)
            return res
        def get_possible_match(self, pilot_id, q=None):
            gwjob=None
            if pilot_id in self.pilot_reg_list.keys() :
                p=self.pilot_reg_list[pilot_id]
                p.primitive_lock.acquire()
                gwjob=p.gwjob
                p.primitive_lock.release()
            if q!=None : q.put(gwjob)
            return gwjob
        def set_possible_match(self, gwjob, q=None):
            (rsl, pilot_id)= GWJobWrapper().getmatch(gwjob)
            if pilot_id in self.pilot_reg_list.keys() :
                p=self.pilot_reg_list[pilot_id]
                p.primitive_lock.acquire()
                p.gwjob=gwjob
                p.primitive_lock.release()
            if q!=None : q.put(pilot_id)
            return pilot_id
        def remove_possible_match(self, gwjob, q=None):
            (rsl, pilot_id)= GWJobWrapper().getmatch(gwjob)
            if pilot_id in self.pilot_reg_list.keys() :
                p=self.pilot_reg_list[pilot_id]
                p.primitive_lock.acquire()
                p.gwjob=None
                p.primitive_lock.release()
            if q!=None : q.put(pilot_id)
            return pilot_id
        def clean_pilot_reg_list(self, max_failed_updates, expire_period):
            while True:
                time.sleep(expire_period)
                for p in self.pilot_reg_list.values():
                    p.primitive_lock.acquire()
                    if p.isfree==False:
                        p.failed_updates+=1
                        if p.failed_updates > max_failed_updates:
                            pilot_id=p.state['HOSTNAME']                            
                            job=p.gwjob
                            if job!=None:      
                                (rsl, pilot_match)= GWJobWrapper().getmatch(job)
                                if pilot_id==pilot_match:
                                    if self.logger!=None :  self.logger.info("Job pilot match detected: job_id=%s" % job.jid)
                                    status=GWJobWrapper().getstatus(job)                     
                                    if status=='PENDING' or status=='ACTIVE':
                                        if self.logger!=None :  self.logger.info("Failing job_id=%s" % job.jid)
                                        GWJobWrapper().setsignal(job, None)
                                        GWJobWrapper().setmatch(job, (None,None))
                                        GWJobWrapper().setstatus(job, 'FAILED')
                            if self.logger!=None :  self.logger.info("Pilot free: HOSTNAME=%s" % pilot_id)
                            self.map.pop(p.state['PILOT_HASH_NAME'])
                            p.free()
                    p.primitive_lock.release()
                self._write_used_PilotState()
        def _write_used_PilotState(self):
            data=[]
            for p in self.pilot_reg_list.values():
                p.primitive_lock.acquire()
                if p.isfree==False :
                    data.append(p.print_state()+'\n')
                p.primitive_lock.release()
            f=lock_and_open_file(self.im_file,'w+')
            trunc_and_write_locked_file(f,data)
            f.close()
    class _PilotListContainer(object):
        def  __init__ (self, max_num_pilots): 
            self.pilot_reg_list={}
            user=os.environ['USER']
            for i in range(0, max_num_pilots-1):
                virtual_hostname='pilot_'+user+'_'+str(i)
                self.pilot_reg_list[virtual_hostname]=PilotList._RegisteredPilot(virtual_hostname)
            self.last_used=self.pilot_reg_list[self.pilot_reg_list.keys()[0]]
            self.map={}
    def __init__(self,max_num_pilots,max_failed_updates,expire_period,fifo_output,logger=None):
        self.logger=logger
        self.pilot_list_cont=self._PilotListContainer(max_num_pilots)
        environment=os.environ
        user=environment['USER']
        gw_path=os.environ['GW_LOCATION']
        self.im_file=gw_path+'/var/'+'im_pilot_file_'+user
        f=open(self.im_file,'w')
        f.truncate()
        f.close()
        self._periodical_clean_pilot_reg_list(max_failed_updates,expire_period)
        cmd=gw_path+'/libexec/bash/GWutils/gw_im_mad_generate_diff.sh ' + self.im_file + ' ' + fifo_output + ' ' + str(expire_period/2)
        os.system(cmd + '  2>&1>&/dev/null &')
    def _execfunction(self, func, argument):
        q=Queue(1)
        new_thread = self._PilotListManager(self.pilot_list_cont, self.im_file, self.logger, priv_func=func, args=(argument,q))
        new_thread.start()
        return q.get()
    def include_new_pilot(self, argument):    
        return self._execfunction('include_new_pilot', argument)
    def is_a_valid_pilot(self, argument):
        return self._execfunction('is_a_valid_pilot', argument)
    def update_pilot(self, argument):    
        return self._execfunction('update_pilot', argument)
    def get_possible_match(self, argument):
        return self._execfunction('get_possible_match', argument)
    def set_possible_match(self, argument):
        return self._execfunction('set_possible_match', argument)
    def remove_possible_match(self, argument):
        return self._execfunction('remove_possible_match', argument)
    def _periodical_clean_pilot_reg_list(self,max_failed_updates, expire_period):
        new_thread = self._PilotListManager(self.pilot_list_cont, self.im_file, self.logger, priv_func='clean_pilot_reg_list', kwargs={'max_failed_updates':max_failed_updates, 'expire_period':expire_period})
        new_thread.start()
class ResourceList(PilotList):
    def __init__(self,ops,logger):
        max_num_pilots=ops.max_num_jobs
        PilotList.__init__(self,max_num_pilots,ops.max_failed_updates,ops.pilot_update_time,ops.fifo_output,logger)
        from GWpilot_server import PilotServer 
        num_threads=int(max_num_pilots/5) 
        self.pilot_server=PilotServer(self, num_threads, logger, ops.https, ops.port)
        self.pilot_server.start()
