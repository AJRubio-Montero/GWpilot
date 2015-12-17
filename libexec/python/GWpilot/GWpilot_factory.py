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

import sys,logging,os
from GWpilot_PilotGenerator import PilotGenerator
from GWutils.easyDRMAA.DRMAA_controller import DRMAA_controller 
import socket
FQDN=socket.getfqdn()
def main(*args):
    try:
        import optparse
        p = optparse.OptionParser(usage="%prog [-h] [ -s -p [-f] [-t] [--nosec] ]", version="%prog 0.2")
        groupserver = optparse.OptionGroup(p, "Pilot server URL",
                                    "They are needed values")
        groupserver.add_option('-n','--total_pilot_number', type="int", dest="total_pilots", help="total number of managed pilots")
        groupserver.add_option('-o','--overload_with_pilots', type="int", dest="overload",default=0, help="overload with a number of pilots")
        groupserver.add_option('-s','--pilot_server', type="str", dest="pilot_server",default=FQDN, help="GWpilot Server hostname")
        groupserver.add_option('-p','--em_server_port', type="int", dest="em_server_port", help="GWpilot Server port.")
        p.add_option_group(groupserver)
        groupaccess = optparse.OptionGroup(p, "Access options",
                                    "They are optional values")
        groupaccess.add_option('-f','--max_failed_access', type="int", dest="max_failed_access", default=15 , help="Maximun failed accesses to GWpilot server. Default is 15.")
        groupaccess.add_option('-t','--webtimeout', type="int", dest="webtimeout", default=1, help="Timeout of access operations to GWpilot server. Default is 1.")
        groupaccess.add_option('-i','--access_interval', type="int", dest="access_interval", default=45, help="Interval between each pilot access to GridWay Pilot Server. Default is 45.")
        p.add_option_group(groupaccess)
        groupsched = optparse.OptionGroup(p, "Scheduling options",
                                    "They are optional values")
        groupsched.add_option('-r','--rank_expression', type="str", dest="rank", default=None, help="GW Rank expression used in provisioning (between \"\"). Default is None.")
        groupsched.add_option('-m','--monitoring_vars', type="str", dest="mon_vars", default=None, help="Additional Tags that will be monitored in pilots and will be added to description of real sites. List of values (point-separated without blanks). Default is None.")
        groupsched.add_option('-d','--default_values_mon_vars', type="str", dest="default_mon", default="", help="List of ordered values that correspond with default values for PILOT_\$GW_USER_VAR\<num\> (point-separated without blanks).")
        groupsched.add_option('-w','--writting_fifo', type="str", dest="fifo_output", default=None, help="FIFO where pilot updates the changes en monitored tags are written to advertise IM MAD (required if additional tags are monitored)")
        groupsched.add_option('-c','--requirement_expression', type="str", dest="requirement", default=None, help="Expression to constraint pilot submission")
        groupsched.add_option('-g','--percent_guided', type="int", dest="percent_guided", default=50, help="% of guided pilots")        
        p.add_option_group(groupsched)
        groupsec = optparse.OptionGroup(p, "Security and Misc options",
                                        "Use personal certificate to secure encrypt comunications (It is the default)")
        groupsec.add_option('--nosec', action="store_false", dest="https", default=True, help="Do not use encryptation.")
        groupsec.add_option('-l','--log_path', dest="log_path", type="str", default="", help="Path for log")
        p.add_option_group(groupsec)
        o, a = p.parse_args()
        mon_vars_list=None  
        try:        
            if o.fifo_output!=None and o.mon_vars!=None:
                mon_vars_list= o.mon_vars.split('.')
            else:
                mon_vars_list=None
        except Exception, e:
            print >> sys.stderr, e.__class__, e
        logger=None
        if o.log_path!="":
            logger=logging.getLogger("GW_factory")
            log_handler=logging.FileHandler(o.log_path)
            log_formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
            log_handler.setFormatter(log_formatter)
            logger.addHandler(log_handler)
            logger.setLevel(logging.INFO)
        DRMAA_app=PilotGenerator(o.overload, o.pilot_server, o.em_server_port, o.max_failed_access, o.webtimeout, 
                      o.access_interval, o.https, o.rank, mon_vars_list, o.default_mon, o.fifo_output, logger)
        DRMAA_run=DRMAA_controller(DRMAA_app, o.total_pilots, 1, 1, logger)
    except Exception, e:
        print >> sys.stderr, e.__class__, e
        sys.exit(-1)
if __name__ == '__main__':
    sys.exit(main(*sys.argv))
