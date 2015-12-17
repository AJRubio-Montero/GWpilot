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

#!/usr/bin/env python
import urllib
import urllib2
import httplib
import os, shutil, sys, time
import Queue
import traceback
import random
import threading
import datetime
import socket
import subprocess
from pilot_aux_funcs import *
def transfer_grid_file(orig,dest):
    cmd='globus-url-copy ' + orig + ' ' + dest        
    tries=3
    while tries>0:
        try:
            res = run_command(cmd, 0)
            return res
        except commandError, e:
            print >> sys.stderr, e.__class__, e.returncode, e.command, e.res
            tries=tries-1
            print >> sys.stderr, "transfer_grid_file(), tries remaining = ", tries 
        except Exception, e:
            print >> sys.stderr, e.__class__, e               
            tries=tries-1
            print >> sys.stderr, "transfer_grid_file(), tries remaining = ", tries 
        time.sleep(1)
    raise Exception('Tries exceed for command' + cmd)
class PilotState(object):
    def __init__(self, default_mon_var_list=[],state=None,gw_host=None,gw_user=None):
        self.gw_host=gw_host
        self.pilotpid=os.getpid()
        self.failed_updates=0
        if state!=None:
            self.state=state
            return None
        self.state={}
        self.fifos={}
        environ=os.environ
        self.state={
                    'HOSTNAME':None,
                    'ARCH':get_sys_arch(), 
                    'OS_NAME':get_sys_os_name(), 
                    'OS_VERSION':get_sys_os_version(), 
                    'CPU_MODEL':get_sys_cpu_model(), 
                    'CPU_MHZ':get_sys_cpu_mhz(), 
                    'CPU_FREE':get_sys_cpu_free_1min(), 
                    'CPU_SMP':1, 
                    'NODECOUNT':1, 
                    'SIZE_MEM_MB':get_sys_size_mem(), 
                    'FREE_MEM_MB':get_sys_free_mem(self.pilotpid,None), 
                    'SIZE_DISK_MB':1000,
                    'FREE_DISK_MB':1000,
                    'FORK_NAME':"jobmanager-pilot", 
                    'LRMS_NAME':"jobmanager-pilot", 
                    'LRMS_TYPE':"pilot", 
                    'QUEUE_NAME[0]':"default", 
                    'QUEUE_NODECOUNT[0]':1, 
                    'QUEUE_FREENODECOUNT[0]':1, 
                    'QUEUE_MAXTIME[0]':0, 
                    'QUEUE_MAXCPUTIME[0]':0, 
                    'QUEUE_MAXCOUNT[0]':0,
                    'QUEUE_MAXRUNNINGJOBS[0]':1, 
                    'QUEUE_MAXJOBSINQUEUE[0]':1, 
                    'QUEUE_STATUS[0]':"Production", 
                    'QUEUE_DISPATCHTYPE[0]':"Immediate", 
                    'QUEUE_PRIORITY[0]':"NULL",
                    'QUEUE_JOBWAIT[0]':0,
                    'QUEUE_JOBRUN[0]':0,
                    'PILOT_HASH_NAME':environ['GW_HOSTNAME']+get_sys_hostname()+str(random.randrange(1,1000000)), 
                    'PILOT_REAL_HOSTNAME':get_sys_hostname(),
                    'PILOT_REAL_RESOURCE':environ['GW_HOSTNAME'],
                    'PILOT_OWNER_DN':environ['GW_USER'],
                    'PILOT_MEM_MB':get_sys_mem_MB(self.pilotpid), 
                    'PILOT_MEM_USED':get_sys_pmem(self.pilotpid), 
                    'PILOT_CPU_USED':get_sys_pcpu(self.pilotpid),  
                     'PILOT_KERNEL':get_sys_kernel(),
                     'PILOT_BOGOMIPS':get_sys_bogomips(),
                    }
        fifo_path_aux=os.getcwd()+'/fifos/'
        os.mkdir(fifo_path_aux)        
        str_user_var_aux='PILOT_'+environ['GW_USER']+'_VAR_'
        for i in range(0,10):
            user_var = str_user_var_aux+str(i)
            fifo_path = fifo_path_aux+user_var
            self.fifos[user_var] = fifo_path
            try:  
                os.mkfifo(fifo_path)
            except Exception, e:
                continue
        if default_mon_var_list!=[]:
            index=0
            for v in default_mon_var_list:
                self.state['PILOT_'+environ['GW_USER']+'_VAR_'+str(index)] = v
                index+=1
    def updateStateBulk(self,dicc):
        for d in dicc.items() :
            self.state[d[0]]=d[1]
        self.failed_updates=0
    def updateState(self,job_id=None):
        self.updateWNstate()
        if job_id!=None :
            self.state['JOB_ID']=job_id
        self.failed_updates=0
    def updateWNstate(self, job_pid):
        self.state['CPU_FREE']=get_sys_cpu_free_1min()
        self.state['SIZE_MEM_MB']=get_sys_size_mem() 
        self.state['FREE_MEM_MB']=get_sys_free_mem(self.pilotpid,job_pid), 
        if job_pid!=None:
            self.state['PILOT_JOB_MEM_MB']=get_sys_mem_MB(job_pid) 
            self.state['PILOT_JOB_MEM_USED']=get_sys_pmem(job_pid) 
            self.state['PILOT_JOB_CPU_USED']=get_sys_pcpu(job_pid) 
        else:
            self.state['PILOT_JOB_MEM_MB']=0 
            self.state['PILOT_JOB_MEM_USED']=0 
            self.state['PILOT_JOB_CPU_USED']=0 
        self.state['PILOT_MEM_MB']=get_sys_mem_MB(self.pilotpid) 
        self.state['PILOT_MEM_USED']=get_sys_pmem(self.pilotpid) 
        self.state['PILOT_CPU_USED']=get_sys_pcpu(self.pilotpid) 
        for key in self.fifos.keys():
            fifo_path=self.fifos[key]
            try: 
                io = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
                buffer=None 
                try:
                    buffer = os.read(io, 256)
                except OSError as err:
                    if err.errno == OSError.EAGAIN or err.errno == OSError.EWOULDBLOCK:
                        buffer = None
                    else:
                        buffer = None             
                if buffer is not None:
                    buffer=buffer.strip()
                    if buffer:
                        last_line=buffer.splitlines().pop()
                        last_line=last_line.strip() 
                        if last_line:
                            self.state[key] = last_line
                            print 'buffer:$'+last_line+'$'
                os.close(io)
            except OSError as err:
                print "Error reading: " + str(fifo_path)
    def print_state(self):    
        s=''
        for m in self.state.items():
            if str(m[1]).isdigit():
                s+=str(m[0])+'='+str(m[1])+' '
            else:
                s+=str(m[0])+'="'+str(m[1])+'" '
        return s
class JobState(object):
    def _write_environment(self,dicc,env_path):
        f=open(env_path,'w')
        for m in dicc.items():
            s=str(m[0])+'="'+str(m[1])+'"\n'    
            f.write(s)        
        f.close()        
    def parse_job_rsl(self,rsl_nsh):        
        rsl_nsh=rsl_nsh.replace('&','',1)
        rsl_nsh=rsl_nsh.replace(')(',',\'')
        rsl_nsh=rsl_nsh.replace('(','{\'')
        rsl_nsh=rsl_nsh.replace(')','}')
        rsl_nsh=rsl_nsh.replace('=','\':')
        rsl_nsh=rsl_nsh.replace(' ','\':')
        job_dict=dict(eval(rsl_nsh))
        return job_dict
    def __init__(self,rsl,fifos={}):
        self.job_params={}
        self.job_params=self.parse_job_rsl(rsl)
        self.job_state='ACTIVE'
        self.job_id=str(self.job_params['environment']['GW_JOB_ID'])
        self.job=None
        self.fifos=fifos
    def execute_job(self):    
        try:
            job_path=os.getcwd()+'/gw_job_id_'+ str(self.job_id) +'/'
            os.mkdir(job_path)
            exec_path = job_path + self.job_params['executable'].rsplit('/',1).pop()
            transfer_grid_file(self.job_params['executable'],'file:'+exec_path)
            os.chmod(exec_path,0777)      
            environ=os.environ
            for k in environ.keys():
                if k.startswith('GW_'):
                    environ[k]=''
            for e in self.job_params['environment'].items():
                environ[e[0]]=str(e[1])
            environ['HOME']=job_path    
            for key in self.fifos.keys():    
                environ[key]=self.fifos[key]
            stdout_path=job_path + 'stdout.txt'
            stderr_path=job_path + 'stderr.txt'
            f_stdout_path = file(stdout_path,'w')
            f_stderr_path = file(stderr_path,'w')
            failed=False
            try:                     
                self.job = subprocess.Popen(args=[' ',self.job_params['arguments']],executable=exec_path,stdout=f_stdout_path, stderr=f_stderr_path, env=environ, cwd=job_path)
                self.job.wait()
            except Exception, e:
                print >> sys.stderr, e.__class__, e
                print >> sys.stderr, "JobState.execute_job() / subprocess.Popen ERROR"
                failed=True
            f_stdout_path.close()
            f_stderr_path.close()
            transfer_grid_file('file:'+stdout_path,self.job_params['stdout'])
            transfer_grid_file('file:'+stderr_path,self.job_params['stderr'])
            shutil.rmtree(job_path)
            if not failed : self.job_state='DONE'
        except Exception, e:
            print >> sys.stderr, e.__class__, e
            print >> sys.stderr, "JobState.execute_job() / transfer files ERROR"
            self.job_state='FAILED'
    def signal_to_job(self,signal):
        if self.job!=None :
            os.kill(self.job.pid,9)
class Pilot(PilotState):
    user_cert=os.getcwd()+'/.user_c'
    def __init__(self, gw_server, em_server_port, max_failed_access=10, webtimeout=3, access_interval=10, https=True, default_mon_var_list=[]):
        PilotState.__init__(self,default_mon_var_list) 
        self.gw_server=gw_server           
        self.em_server_port=em_server_port
        self.max_failed_access=max_failed_access
        self.failed_access=0
        self.max_attemps_for_jobs=max_failed_access
        self.attemps_for_jobs=0
        self.webtimeout=webtimeout
        socket.setdefaulttimeout(self.webtimeout)
        self.access_interval=access_interval
        self.https=https
        self.job=None
        proxy_file=os.environ['X509_USER_PROXY']
        self.user_key=proxy_file
        cmd='openssl x509 -in ' + proxy_file + ' -out ' + self.user_cert
        run_command(cmd)      
    def _open_pilot_default_url(self, method, dicc):
        url = 'http://' + self.gw_server + ':' + str(self.em_server_port) + method
        data = urllib.urlencode(dicc)
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)
        the_page = response.read()
        return the_page
    def _open_pilot_https_url(self, method, dicc):
        url = 'https://' + self.gw_server + ':' + str(self.em_server_port) + method
        data = urllib.urlencode(dicc)        
        req = urllib2.Request(url, data)
        class HTTPSConnectionFactory:
            def __init__(self, key_file, cert_file):
                self._key_file = key_file
                self._cert_file = cert_file
            def __call__(self, hostport):
                return httplib.HTTPSConnection(hostport,key_file=self._key_file, cert_file=self._cert_file)
        class NewHTTPSHandler(urllib2.HTTPSHandler):
            def __init__(self, user_key, user_cert):
                urllib2.HTTPSHandler.__init__(self)
                self.connection_class=HTTPSConnectionFactory(user_key, user_cert)
            def https_open(self, req):
                return self.do_open(self.connection_class, req)
        https_handler = NewHTTPSHandler(self.user_key, self.user_cert)
        opener = urllib2.build_opener(https_handler)
        urllib2.install_opener(opener)
        response = urllib2.urlopen(req)
        the_page = response.read()
        response.close()
        return the_page
    def _open_pilot_url(self, method, dicc):
        t1 = datetime.datetime.now() 
        if self.https :
            result=self._open_pilot_https_url(method,dicc)
        else :
            result=self._open_pilot_default_url(method,dicc)
        t2 = datetime.datetime.now() 
        return ( result , getTime(t2) - getTime(t1))
    def _handle_access_to_GW_exceptions(self, e , message1, message2):
        print >> sys.stderr, e.__class__, e
        print >> sys.stderr, message1
        print >> sys.stderr, message2
        self.failed_access+=1
        if self.max_failed_access < self.failed_access :
            print >> sys.stderr, 'Too many failed accesses to GW server'
            self.state['QUEUE_FREENODECOUNT[0]']= 0
            self.state['QUEUE_JOBWAIT[0]']=1 
            self.state['QUEUE_JOBRUN[0]']=1 
            self.updateJobState()
            self.updatePilotState()
            sys.exit(-1)
    def notifyPilot(self):
        method = '/notifyPilot?'      
        new_hostname_id=''
        try:
            (new_hostname_id, lag) =  self._open_pilot_url(method, self.state)       
            self.state['PILOT_LAG']=lag
            new_hostname_id=new_hostname_id.strip('\n')
            if new_hostname_id.find('ERROR')> -1 :
                raise Exception('Wrong result')
            print >> sys.stderr, new_hostname_id
            self.state['HOSTNAME']= new_hostname_id
            print >> sys.stderr, 'Pilot registered. \n' + 'pilot_hash_name: ' + self.state['PILOT_HASH_NAME'] 
            self.failed_access=0
            return True
        except Exception, e:
            self._handle_access_to_GW_exceptions(e,"Pilot.notifyPilot() ERROR", new_hostname_id)
            return False
    def updatePilotState(self):
        method = '/updatePilotState?'
        result=''       
        try:
            job_pid=None
            if self.job!=None : 
                if self.job.job!=None: 
                    job_pid=self.job.job.pid
            self.updateWNstate(job_pid) 
            (result, lag) =  self._open_pilot_url(method, self.state)       
            self.state['PILOT_UPDT_LAG']=lag
            if result.find('ERROR')> -1 :
                raise Exception('Wrong result')
            self.failed_access=0                
            return True
        except Exception, e:
            self._handle_access_to_GW_exceptions(e,"Pilot.updatePilotState() ERROR", result)
            return False
    def updateJobState(self):
        if self.job==None :
            return False
        else :
            method = '/updateJobState?'
            res=''
            try:
                dicc={'job_id':self.job.job_id, 'job_state':self.job.job_state , 'HOSTNAME':self.state['HOSTNAME'], 'PILOT_HASH_NAME':self.state['PILOT_HASH_NAME']}
                (res, lag) =  self._open_pilot_url(method, dicc)    
                self.state['PILOT_GETJOB_LAG']=lag
                if res.find('ERROR')> -1 :
                    raise Exception('Wrong result')   
                self.failed_access=0
                return True
            except Exception, e:
                self._handle_access_to_GW_exceptions(e,"Pilot.updateJobState() ERROR", res)
                return False
    def getJobSignals(self):
        if self.job==None :
            return False
        else:
            method = '/getJobSignals?'
            signal=''
            try:
                dicc={'job_id':self.job.job_id, 'HOSTNAME':self.state['HOSTNAME'], 'PILOT_HASH_NAME':self.state['PILOT_HASH_NAME']}
                (signal, lag) =  self._open_pilot_url(method, dicc)
                self.state['PILOT_GETJOB_LAG']=lag
                signal=signal.strip('\n')
                if signal.find('ERROR')> -1 :
                    raise Exception('Wrong result')   
                if signal!='' :
                    self.job.signal_to_job(signal)
                self.failed_access=0     
                return True
            except Exception, e:
                self._handle_access_to_GW_exceptions(e,"Pilot.getJobSignals() ERROR", signal)     
                return False
    def getJob(self):
        if self.job!=None :
            return False
        else:
            method = '/getJob?'
            rsl=''
            try:      
                dicc={'HOSTNAME':self.state['HOSTNAME'],'PILOT_HASH_NAME':self.state['PILOT_HASH_NAME']}
                (rsl, lag) =  self._open_pilot_url(method, dicc)       
                self.state['PILOT_GETJOB_LAG']=lag
                print >> sys.stderr, 'get rsl=', rsl
                print >> sys.stderr, 'failed_access=', self.failed_access,' attems_for_jobs=',self.attemps_for_jobs    
            except Exception, e:
                self._handle_access_to_GW_exceptions(e,"Pilot.getJob() ERROR", rsl)                                
                return False
            try:
                rsl=rsl.strip('\n')
                if rsl!='' :
                    self.job = JobState(rsl,self.fifos)
                    self.failed_access=0
                    self.attemps_for_jobs=0                    
                    self.state['QUEUE_FREENODECOUNT[0]']= 0
                    self.state['QUEUE_JOBRUN[0]']=1 
                    self.updatePilotState()
                    while not self.updateJobState():
                        time.sleep(self.access_interval)
                        pass
                    self.job.execute_job()
                    self.state['QUEUE_FREENODECOUNT[0]']= 1
                    self.state['QUEUE_JOBRUN[0]']=0
                    while not self.updateJobState():
                        time.sleep(self.access_interval)
                        pass
                    self.job = None
                    self.updatePilotState()
                else:
                    self.attemps_for_jobs+=1
                    if self.max_attemps_for_jobs < self.attemps_for_jobs:
                        self.state['QUEUE_FREENODECOUNT[0]']= 0
                        self.state['QUEUE_JOBWAIT[0]']=1 
                        self.state['QUEUE_JOBRUN[0]']=1 
                        self.updatePilotState()
                        print >> sys.stderr, 'Number of attemps for jobs excedeed'
                        sys.exit(0) 
                return True
            except SystemExit, ex:
                sys.exit(ex.code)
            except Exception, e:
                self.state['QUEUE_FREENODECOUNT[0]']= 1
                self.state['QUEUE_JOBRUN[0]']=0
                if self.job!=None :
                    while not self.updateJobState():
                        time.sleep(self.access_interval)
                        pass
                self.job = None
                self.updatePilotState()
                return False    
def main_function(pilot):
    def parall_func_1(q): 
        try:
            while True :
                print >> sys.stderr, 'Executing parall_func_1'
                time.sleep(pilot.access_interval)
                if pilot.job==None:
                    pilot.getJob()
        except SystemExit, ex :
            q.put(ex.code)
        except:
            traceback.print_exc(file=sys.stderr)
            q.put(1)
    def parall_func_2(q):
        try:
            while True:
                print >> sys.stderr, 'Executing parall_func_2'
                time.sleep(pilot.access_interval)
                if pilot.job!=None :
                    pilot.getJobSignals()
                pilot.updatePilotState()
        except SystemExit, ex :
            q.put(ex.code)
        except:
            traceback.print_exc(file=sys.stderr)
            q.put(1)
    while not pilot.notifyPilot():
        time.sleep(pilot.access_interval)
        pass
    time.sleep(pilot.access_interval)
    q=Queue.Queue(1)
    t1=threading.Thread(target=parall_func_1, args=(q,))
    t2=threading.Thread(target=parall_func_2, args=(q,))
    t1.start()
    t2.start()
    code=q.get()
    print >> sys.stderr, 'Exit Code: ', code 
    os._exit(code)
def main(*args):
    try:
        import optparse
        p = optparse.OptionParser(usage="%prog [-h] [ -s -p [-f] [-t] [--nosec] ]", version="%prog 0.2")
        groupserver = optparse.OptionGroup(p, "Pilot server URL",
                                    "They are needed values")       
        groupserver.add_option('-s','--gw_server', type="string", dest="gw_server", help="GridWay  pilot server hostname.")
        groupserver.add_option('-p','--em_server_port', type="int", dest="em_server_port", help="GridWay pilot server port.")
        p.add_option_group(groupserver)
        groupaccess = optparse.OptionGroup(p, "Access options",
                                    "They are optional values")
        groupaccess.add_option('-f','--max_failed_access', type="int", dest="max_failed_access", default=10 , help="Maximun failed accesses to GridWay pilot server. Default is 10.")
        groupaccess.add_option('-t','--webtimeout', type="int", dest="webtimeout", default=3, help="Timeout of access operations to GridWay pilot server. Default is 3.")
        groupaccess.add_option('-i','--access_interval', type="int", dest="access_interval", default=10, help="Interval between each pilot access to GridWay Pilot Server. Default is 10.")
        p.add_option_group(groupaccess)
        groupsec = optparse.OptionGroup(p, "Security options",
                                        "Use personal certificate to secure encrypt comunications (It is the default)")
        groupsec.add_option('--nosec', action="store_false", dest="https", default=True, help="Do not use encryptation.")
        p.add_option_group(groupsec)
        groupothers = optparse.OptionGroup(p, "Other options",
                                        "Optional values")
        groupothers.add_option('-d','--default_values_mon_vars', type="str", dest="default_values_mon_vars", default="", help="List of ordered values that correspond with default values for PILOT_\$GW_USER_VAR\<num\>. (point.separated values without blanks)")
        p.add_option_group(groupothers)
        o, a = p.parse_args()
        default_mon_list=[]
        try:
            if o.default_values_mon_vars != "":
                default_mon_list= o.default_values_mon_vars.split('.')
        except Exception, e:
            print >> sys.stderr, e.__class__, e
        pilot=Pilot(o.gw_server,o.em_server_port,o.max_failed_access,o.webtimeout,o.access_interval,o.https,default_mon_list)
        print >> sys.stderr, pilot.print_state()
        main_function(pilot)
    except Exception, e:
        print >> sys.stderr, e.__class__, e
        sys.exit(-1)
if __name__ == '__main__':
    sys.exit(main(*sys.argv))
