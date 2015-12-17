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

import os, time, threading, subprocess
from GWutils.easyDRMAA.DRMAA_app_abstraction import GridApplication
import xml.dom.minidom
environment=os.environ
GW_LOCATION=environment['GW_LOCATION']
USER=environment['USER']
import socket
FQDN=socket.getfqdn()
class PilotGenerator(GridApplication):
    def _include_pilot_var_in_site(self):
        resources={} 
        sites={}
        cmd=GW_LOCATION+'/bin/gwhost -f > '+ self.tmp_file
        os.system(cmd)
        f=open(self.tmp_file,'r')
        lines=f.readlines()
        f.close()
        HOST_ID=None
        for line in lines:
            line=line.rstrip('\n')            
            if not line.strip(): 
                HOST_ID=None
            elif HOST_ID==None:
                key,value=line.split('=',1)
                HOST_ID=value
                resources[HOST_ID]={}
            else:
                key,value=line.split('=',1)
                if key=='PILOT_REAL_RESOURCE' or key=='HOSTNAME' or (key in self.additional_mon_vars):
                    resources[HOST_ID][key]=value
        pilot_num_str='PILOT_'+USER+'_NUM'
        for host in resources.keys():           
            if resources[host].has_key('PILOT_REAL_RESOURCE'):
                HOSTNAME=resources[host]['PILOT_REAL_RESOURCE']
                if not sites.has_key(HOSTNAME): 
                    sites[HOSTNAME]={} 
                    sites[HOSTNAME][pilot_num_str]=0 
                sites[HOSTNAME][pilot_num_str] = sites[HOSTNAME][pilot_num_str] + 1
                if self.additional_mon_vars!=None:
                    for mon_var in self.additional_mon_vars:                         
                        if resources[host].has_key(mon_var):                        
                            value_aux=resources[host][mon_var]
                            try:
                                value=float(value_aux)  
                                if value != 0 and value < 65535.0: 
                                    if sites[HOSTNAME].has_key(mon_var):
                                        num = sites[HOSTNAME][mon_var][1] + 1.0
                                        sites[HOSTNAME][mon_var][1]= num
                                        avg = sites[HOSTNAME][mon_var][0]
                                        sites[HOSTNAME][mon_var][0]= ( float(avg) * (num - 1) + float(value) ) / num
                                    else:
                                        sites[HOSTNAME][mon_var]={}
                                        sites[HOSTNAME][mon_var][0]=value  
                                        sites[HOSTNAME][mon_var][1]=1.0    
                            except ValueError:
                                if self.logger!=None :  self.logger.error("Error %s is not int or float" % str(value))
                                continue
                            except:
                                if self.logger!=None :  self.logger.error("Unknow error: %s" % sys.exc_info()[0])
                                continue
            else:     
                HOSTNAME=resources[host]['HOSTNAME']
                if not sites.has_key(HOSTNAME): 
                    sites[HOSTNAME]={}
                    sites[HOSTNAME][pilot_num_str]=0 
                sites[HOSTNAME]['HOST_ID']=host      
        for HOSTNAME in sites.keys():
            if sites[HOSTNAME].has_key('HOST_ID'):
                if sites[HOSTNAME][pilot_num_str]>0:
                    str_aux=pilot_num_str+ '=' + str(sites[HOSTNAME][pilot_num_str])
                    for k in sites[HOSTNAME].keys() :
                        if k!='HOST_ID' and k!=pilot_num_str:  
                            str_aux=str_aux+' '+ str(k)+'='+str(int(sites[HOSTNAME][k][0]))
                    cmd='echo \'MONITOR '+ sites[HOSTNAME]['HOST_ID'] +' SUCCESS '+ str_aux +' \' >> ' + self.fifo_output
                    os.system(cmd)
                    aux='echo MONITOR '+ sites[HOSTNAME]['HOST_ID'] +' SUCCESS '+ str_aux   
                    if self.logger!=None :  self.logger.warn("Monitor:  %s is not int or float" % aux)
    def _run_Popen(self, command):        
        p = subprocess.Popen(command + ' 2>&1', shell = True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        p.wait()
        res = p.communicate()[0]
        if p.returncode != 0:
            res=""
        return res
    def _get_num_pend_and_dispached_to_pilot(self):
        l=[]
        cmd=GW_LOCATION+'/bin/gwps -u '+ USER + ' -s i -o J -n'
        laux1=self._run_Popen(cmd)
        laux2=laux1.replace(' ','').split('\n')
        laux2.pop() 
        l=l+laux2
        cmd=GW_LOCATION+'/bin/gwps -u '+ USER + ' -s p -o J -n'
        laux1=self._run_Popen(cmd)
        laux2=laux1.replace(' ','').split('\n')
        laux2.pop()
        l=l+laux2
        cmd=GW_LOCATION+'/bin/gwps -u '+ USER + ' -s w -o J -n'
        laux1=self._run_Popen(cmd)
        laux2=laux1.replace(' ','').split('\n')
        laux2.pop()
        l=l+laux2
        cmd=GW_LOCATION+'/bin/gwps -u '+ USER + ' -s m -o J -n'
        laux1=self._run_Popen(cmd)
        laux2=laux1.replace(' ','').split('\n')
        laux2.pop()
        l=l+laux2
        jobs=0
        for p in l:
                cmd='grep REQUIREMENTS ' + GW_LOCATION + '/var/' + p +'/job.template'
                requirements=self._run_Popen(cmd)
                if requirements.count('!(LRMS_NAME="jobmanager-pilot")')==0 and requirements.count('LRMS_NAME="jobmanager-pilot"')>0 : 
                    jobs+=1
        return jobs
    def _monitor_pending_for_pilot(self, args=None):
        while True:
            time.sleep(50)
            self.pend_disp_for_pilot=self._get_num_pend_and_dispached_to_pilot()
    def _monitor_vars_in_pilot(self, args=None):
        while True:
            time.sleep(60)
            self._include_pilot_var_in_site()
    def __init__(self, overproduction=0, pilot_server=FQDN, em_server_port=24999, max_failed_access=15, webtimeout=1, access_interval=45, https=True, rank=None, additional_mon_vars=None, default_mon="",  fifo_output=None, logger=None):
        GridApplication.__init__(self)
        self.overprod=overproduction
        self.pilot_server=pilot_server
        self.em_server_port = em_server_port  
        self.max_failed_access = max_failed_access  
        self.webtimeout = webtimeout 
        self.access_interval = access_interval 
        self.https=https
        self.rank = rank 
        if additional_mon_vars==None: self.additional_mon_vars = []
        else: self.additional_mon_vars = additional_mon_vars 
        self.ranked=False 
        self.default_mon=default_mon
        self.fifo_output=fifo_output
        self.logger=logger
        aux="Initialised.  rank: " +str(self.rank) +" addtional_mon_vars: "+str(self.additional_mon_vars)+ " default_mon: "+str(self.default_mon)
        if self.logger!=None :  self.logger.info("Pilot Generator: %s " % aux) 
        time.sleep(10) 
        self.pend_disp_for_pilot=0
        self.num_pilots_active=0
        self.tmp_file='/tmp/gwhost_factory.tmp'+ str(os.getpid())
        t1=threading.Thread(target=self._monitor_pending_for_pilot)
        t1.start()
        t2=threading.Thread(target=self._monitor_vars_in_pilot)
        t2.start()
    def generate_template(self):
        if ( self.pend_disp_for_pilot + self.overprod - self.num_pilots_active) <= 0 :
            return (None,None)
        self.num_pilots_active =self.num_pilots_active + 1
        nosec_str='--nosec'
        if self.https: nosec_str=''
        tplt={}
        tplt['NAME']='pilot.jt'
        tplt['EXECUTABLE']='/usr/bin/python'
        if str(self.default_mon)=="":
            tplt['ARGUMENTS']="pilot.py -s "+str(self.pilot_server)+" -p "+str(self.em_server_port)+" -f "+str(self.max_failed_access)+" -t "+str(self.webtimeout)+" -i "+str(self.access_interval)+" "+nosec_str
        else:
            tplt['ARGUMENTS']="pilot.py -s "+str(self.pilot_server)+" -p "+str(self.em_server_port)+" -f "+str(self.max_failed_access)+" -t "+str(self.webtimeout)+" -i "+str(self.access_interval)+" -d "+str(self.default_mon) +" "+nosec_str
        tplt['INPUT_FILES']='file://'+GW_LOCATION+'/libexec/python/GWpilot/pilot.py pilot.py, ' + 'file://'+GW_LOCATION+'/libexec/python/GWpilot/pilot_aux_funcs.py pilot_aux_funcs.py'
        tplt['STDOUT_FILE']='pilot.out.${JOB_ID}'
        tplt['STDERR_FILE']='pilot.err.${JOB_ID}'
        tplt['REQUIREMENTS']='(!(LRMS_NAME="jobmanager-pilot"))'
        tplt['RESCHEDULE_ON_FAILURE']='no'
        tplt['NUMBER_OF_RETRIES']='0'
        tplt['SUSPENSION_TIMEOUT']='600'
        if self.rank != None and self.ranked:
            tplt['RANK']= self.rank        
            self.ranked=False
        else:
            self.ranked=True
        if self.logger!=None :  self.logger.info(tplt['ARGUMENTS']) 
        return (tplt, None)
    def associate_jobid(self,BD_id,job_id):
        return None
    def get_outputs(self,jid_selected,status):
        self.num_pilots_active = self.num_pilots_active - 1
        return None
