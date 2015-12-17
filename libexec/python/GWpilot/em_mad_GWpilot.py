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

import sys,os
import exceptions,traceback
import logging
from threading import *
from GWutils.EMutils.EM_MAD import EM_MAD
from GWpilot_PilotList import ResourceList
environment=os.environ
GW_LOCATION=environment['GW_LOCATION']
USER=environment['USER']
import socket   
FQDN=socket.getfqdn()
def main(*args):
    try:
        import optparse
        p = optparse.OptionParser(usage="%prog [-h] [ [-f] [-i] [--nosec] [-l] ]", version="%prog 0.2")
        groupaccess = optparse.OptionGroup(p, "Access options",
                                    "(They are optional values)")
        groupaccess.add_option('-f','--max_failed_updates', type="int", dest="max_failed_updates", default=10 , help="Maximun failed updates from a pilot to GridWay pilot server. Default is 10.")
        groupaccess.add_option('-i','--pilot_update_time', type="int", dest="pilot_update_time", default=13, help="Maximun interval between each pilot update. Default is 13.")
        groupaccess.add_option('-p','--port', type="int", dest="port", default=24999, help="Port number") 
        p.add_option_group(groupaccess)
        groupsec = optparse.OptionGroup(p, "Security and other options",
                                        "Use personal certificate to secure encrypt comunications (It is the default)")
        groupsec.add_option('--nosec', action="store_false", dest="https", default=True, help="Do not use encryptation.")
        groupsec = optparse.OptionGroup(p, "Logging and profiling options",
                                    "The default is not generate log files and profile files neither")
        groupsec.add_option('-l','--log', action="store_true", dest="wannalog", default=False, help="Generate debug log files")
        groupsec.add_option('-w','--writting_fifo', type="str", dest="fifo_output", help="FIFO where pilot updates are written to advertise IM MAD (required)")
        p.add_option_group(groupsec)
        groupfact = optparse.OptionGroup(p, "Factory options",
                                        "Enable GWpilot Factory")
        groupfact.add_option('-F','--factory', action="store_true", dest="wannafactory", default=False, help="Enable GWpilot Factory")
        groupfact.add_option('-n','--total_pilot_number', type="int", dest="total_pilots", help="total number of managed pilots")
        groupfact.add_option('-o','--overload_with_pilots', type="int", dest="overload",default=0, help="overload with a number of pilots")                
        groupfact.add_option('-t','--webtimeout', type="int", dest="webtimeout", default=1, help="Timeout of access operations to GWpilot server. Default is 1.")
        groupfact.add_option('-r','--rank_expression', type="str", dest="rank", default="", help="GW Rank expression used in provisioning. Default is None.")
        groupfact.add_option('-m','--monitoring_vars', type="str", dest="mon_vars", default="", help="Addtional Tags that will be monitorized in pilots and will be added to description of real sites. List of Comma-separated values. Default is None.")
        groupfact.add_option('-d','--default_values_mon_vars', type="str", dest="default_values_mon_vars", default="", help="List of ordered values that correspond with default values for PILOT_\$GW_USER_VAR\<num\>.")
        groupfact.add_option('-c','--requirement_expression', type="str", dest="requirement", default="", help="Expression to constraint pilot submission")
        groupfact.add_option('-g','--percent_guided', type="int", dest="percent_guided", default=50, help="% of guided pilots")        
        p.add_option_group(groupfact)
        ops, a = p.parse_args()
        em_mad=EM_MAD( ops.pilot_update_time*2, ops.wannalog,ops,ResourceList)
        if ops.wannafactory:          
            cmd='python -OO -m GWpilot.GWpilot_factory'
            cmd+=' -s ' + FQDN
            cmd+=' -p ' + str(ops.port)
            if not ops.https:
                cmd+=' --nosec '
            cmd+=' -f ' + str(ops.max_failed_updates)
            cmd+=' -i ' + str(ops.pilot_update_time)      
            cmd+=' -t ' + str(ops.webtimeout)
            cmd+=' -n ' + str(ops.total_pilots)
            cmd+=' -o ' + str(ops.overload)
            cmd+=' -w ' + str(ops.fifo_output)
            if ops.rank!="":
                cmd+=' -r ' + '\\"'+ops.rank+'\\"'
            if ops.mon_vars !="":
                cmd+=' -m ' + str(ops.mon_vars)
            if ops.default_values_mon_vars!="":
                cmd+=' -d ' + str(ops.default_values_mon_vars)
            if ops.requirement!="":
                cmd+=' -c ' + '\\"'+ops.requirement+'\\"'
            cmd+=' -g '+ str(ops.percent_guided)        
            log_path_gwfactory=GW_LOCATION+'/var/GWpilot_factory_'+USER+'.log.'+str(os.getpid())
            cmd+=' -l ' + log_path_gwfactory
            os.system('echo '+ cmd + ' 2>&1>>'+log_path_gwfactory)           
            os.system(cmd + ' 2>&1>&'+log_path_gwfactory+'_error &' )
        while True:
            em_mad.process_line()
    except exceptions.KeyboardInterrupt, e:
        sys.exit(-1)
    except exceptions.SystemExit, e:
        os._exit(e)
    except Exception, e:
        print "UNKNOW ERROR"
        print e.__class__, e
        traceback.print_exc(file=sys.stdout)
if __name__ == '__main__':
    sys.exit(main(*sys.argv))
