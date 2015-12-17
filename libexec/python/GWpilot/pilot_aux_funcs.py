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

import os
import subprocess
import datetime
class syscommandError(Exception):
    pass
class commandError(syscommandError):
    def __init__(self, command, returncode, res):
        self.command = command
        self.returncode  = returncode
        self.res = res
    def __str__(self):
        return repr(self.returncode)
def run_command(command, expected_returncode=None):
    p = subprocess.Popen(command + ' 2>&1', shell = True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p.wait()
    res = p.communicate()[0]
    if expected_returncode!=None :
        if p.returncode != expected_returncode:
            raise commandError(command, p.returncode, res)
    return res
def get_sys_hostname():
    s=run_command('hostname')
    return s.replace('\n','')
def get_sys_arch():
    s=run_command('uname -m')
    return s.replace('\n','')
def get_sys_cpu_model():
    s=run_command("grep  'model name' /proc/cpuinfo | head -n1 |awk -F': ' '{print $2}'")
    return s.replace('\n','')
def get_sys_cpu_mhz():
    s=run_command("grep  'cpu MHz' /proc/cpuinfo | head -n1 |awk -F': ' '{print $2}'")
    return s.split('.')[0].replace('\n','')
def get_sys_bogomips():
    s=run_command("grep  'bogomips' /proc/cpuinfo | head -n1 |awk -F': ' '{print $2}'")
    return s.split('.')[0].replace('\n','')
def get_sys_core_number():
    s=run_command("grep  'processor' /proc/cpuinfo | wc -l")
    return int(s.replace('\n',''))
NUMBEROFCORES=get_sys_core_number()
def get_sys_pcpu(pid):
    s=run_command("ps h -p "+str(pid)+" S -o pcpu")
    if s=='' : return 0
    return int(float(s.replace('\n','')))
def get_sys_pmem(pid):
    s=run_command("ps h -p "+str(pid)+" S -o pmem")
    if s=='' : return 0
    return int(float(s.replace('\n',''))*NUMBEROFCORES)
def get_sys_size_mem():
    s=run_command("free -m | grep 'Mem' | awk -F' ' '{print $2}'")
    return int(float(s.replace('\n',''))/NUMBEROFCORES)
def get_sys_free_mem(pid,job_pid):
    s=run_command("free -m | grep 'buffers/cache' | awk -F' ' '{print $4}'")
    job_mem=0
    if job_pid!=None : job_mem=get_sys_mem_MB(job_pid)
    corresp_free=get_sys_size_mem()-get_sys_mem_MB(pid)-job_mem
    if corresp_free<0 : corresp_free=0 
    total_free=int(float(s.replace('\n',''))/NUMBEROFCORES)
    if corresp_free<total_free:
        return corresp_free 
    else:
        return total_free
def get_sys_cpu_free_1min():
    s=run_command("cat /proc/loadavg | awk -F ' ' '{print $1}'")
    return int(float(s.replace('\n','')))
def get_sys_mem_MB(pid):
    s=run_command("ps h -p "+str(pid)+" S -o rss")
    if s=='' : return 0
    return int(float(s.replace('\n',''))/1024)
def get_sys_kernel():
    s=run_command('uname -r')
    return s.replace('\n','')
def get_sys_os_name():
    s=run_command('uname -s')
    return s.replace('\n','')
def get_sys_os_version():
    s=run_command('uname -r')
    return s.replace('\n','')
def get_sys_size_disk():
    pass
def get_sys_free_disk():
    pass
def getTime(key):
  return (((key.weekday() * 24 + key.hour) * 60 + key.minute) * 60 + key.second) * 1000000 + key.microsecond
