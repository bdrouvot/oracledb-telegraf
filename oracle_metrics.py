import os
import sys
import cx_Oracle
import argparse
import re
import subprocess

class OraMetrics():
    def __init__(self, user, passwd, sid):
        import cx_Oracle
        self.user = user
        self.passwd = passwd
        self.sid = sid
        self.connection = cx_Oracle.connect( self.user , self.passwd , self.sid )
        cursor = self.connection.cursor()
        cursor.execute("select HOST_NAME from v$instance")
        for hostname in cursor:
            self.hostname = hostname[0]

    def waitclassstats(self, user, passwd, sid):
        cursor = self.connection.cursor()
        cursor.execute("""
        select n.wait_class, round(m.time_waited/m.INTSIZE_CSEC,3) AAS
        from   v$waitclassmetric  m, v$system_wait_class n
        where m.wait_class_id=n.wait_class_id and n.wait_class != 'Idle'
        union
        select  'CPU', round(value/100,3) AAS
        from v$sysmetric where metric_name='CPU Usage Per Sec' and group_id=2
        union select 'CPU_OS', round((prcnt.busy*parameter.cpu_count)/100,3) - aas.cpu
        from
            ( select value busy
                from v$sysmetric
                where metric_name='Host CPU Utilization (%)'
                and group_id=2 ) prcnt,
                ( select value cpu_count from v$parameter where name='cpu_count' )  parameter,
                ( select  'CPU', round(value/100,3) cpu from v$sysmetric where metric_name='CPU Usage Per Sec' and group_id=2) aas
        """)
        for wait in cursor:
            wait_name = wait[0]
            wait_value = wait[1]
            print "oracle_wait_class,host={0},db={1},wait_class={2} wait_value={3}".format(self.hostname,sid,re.sub(' ', '_',wait_name),wait_value)

    def waitstats(self, user, passwd, sid):
        cursor = self.connection.cursor()
        cursor.execute("""
	select
	n.wait_class wait_class, 
       	n.name wait_name,
       	m.wait_count cnt,
       	round(10*m.time_waited/nullif(m.wait_count,0),3) avgms
	from v$eventmetric m,
     	v$event_name n
	where m.event_id=n.event_id
  	and n.wait_class <> 'Idle' and m.wait_count > 0 order by 1 """)
        for wait in cursor:
         wait_class = wait[0]
         wait_name = wait[1]
         wait_cnt = wait[2]
         wait_avgms = wait[3]
         print "oracle_wait_event,host={0},db={1},wait_class={2},wait_event={3} count={4},latency={5}".format(self.hostname,sid,re.sub(' ', '_',wait_class),re.sub(' ', '_', wait_name),wait_cnt,wait_avgms)

    def sysmetrics(self, user, passwd, sid):
        cursor = self.connection.cursor()
        cursor.execute("""
        select metric_name,value,metric_unit from v$sysmetric where group_id=2
        """)
        for metric in cursor:
         metric_name = metric[0]
         metric_value = metric[1]
         print "oracle_sysmetric,host={0},db={1},metric_name={2} metric_value={3}".format(self.hostname,sid,re.sub(' ', '_',metric_name),metric_value)

    def fraused(self, user, passwd, sid):
        cursor = self.connection.cursor()
        cursor.execute("""
        select round((space_used-space_reclaimable)*100/space_limit,1) from v$recovery_file_dest
        """)
        for frau in cursor:
         fra_used = frau[0]
         print "oracle_fra_pctused,host={0},db={1} fra_pctused={2}".format(self.hostname,sid,fra_used)

    def fsused(self):
        fss = ['/']
        for fs in fss:
         df = subprocess.Popen(["df",fs],stdout=subprocess.PIPE)
         output = df.communicate()[0]
         print "oracle_fs_pctused,host={0},fs_name={1} oraclefs_pctused={2}".format(self.hostname,fs,re.sub('%','',output.split("\n")[1].split()[4]))
	
    def tbsstats(self, user, passwd, sid):
        cursor = self.connection.cursor()
        cursor.execute("""
	select tablespace_name,total_space,free_space,perc_used,percextend_used,max_size_mb,free_space_extend
	from(
	select t1.tablespace_name,
       	round(used_space/1024/1024) total_space,
       	round(nvl(lib,0)/1024/1024) free_space,
       	round(100*(used_space-nvl(lib,0))/used_space,1) perc_used,
       	round(100*(used_space-nvl(lib,0))/smax_bytes,1) percextend_used,
       	round(nvl(smax_bytes,0)/1024/1024) max_size_mb,
       	round(nvl(smax_bytes-(used_space-nvl(lib,0)),0)/1024/1024) free_space_extend,
       	nb_ext nb_ext
  	from (select tablespace_name,sum(bytes) used_space from dba_data_files i
         group by tablespace_name) t1,
       	(select tablespace_name,
               sum(bytes) lib,
               max(bytes) max_nb ,
               count(bytes) nb_ext
        from dba_free_space
        group by tablespace_name) t2,
        (select tablespace_name,sum(max_bytes) smax_bytes
        from (select tablespace_name, case when autoextensible = 'YES' then greatest(bytes,maxbytes)
                else bytes end max_bytes
                from dba_data_files i)
        group by tablespace_name ) t3
  	where t1.tablespace_name=t2.tablespace_name(+)
        and t1.tablespace_name=t3.tablespace_name(+)
	)
        """)
        for tbs in cursor:
         tbs_name = tbs[0]
         total_space_mb = tbs[1]
         free_space_mb = tbs[2]
         percent_used = tbs[3]
         percent_used_autoext = tbs[4]
         max_size_mb = tbs[5]
         free_space_autoextend_mb = tbs[6]
         print "oracle_tablespaces,host={0},db={1},tbs_name={2} total_space_mb={3},free_space_mb={4},percent_used={5},percent_used_autoext={6},max_size_mb={7},free_space_autoextend_mb={8}".format(self.hostname,sid,re.sub(' ', '_',tbs_name),total_space_mb,free_space_mb,percent_used,percent_used_autoext,max_size_mb,free_space_autoextend_mb)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help="Username", required=True)
    parser.add_argument('-p', '--passwd', required=True)
    parser.add_argument('-s', '--sid', help="SID", required=True)

    args = parser.parse_args()

    stats = OraMetrics(args.user, args.passwd, args.sid)
    stats.waitclassstats(args.user, args.passwd, args.sid)
    stats.waitstats(args.user, args.passwd, args.sid)
    stats.sysmetrics(args.user, args.passwd, args.sid)
    stats.tbsstats(args.user, args.passwd, args.sid)
    stats.fraused(args.user, args.passwd, args.sid)
    stats.fsused()
