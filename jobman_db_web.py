#import numpy
#from string import Template
import matplotlib
matplotlib.use('agg')
import pylab as plt
#import sys
#import cPickle
#import theano
#import os
#import scipy.io.wavfile
#import itertools
import threading
import SimpleHTTPServer
import BaseHTTPServer
#import logging
import io
import traceback
#import os
#import hashlib
import time
#import base64
import urlparse
import jinja2
import psycopg2
import jobman.sql
import jobman.tools
import datetime
import sys
import urllib
#import json
import sqlalchemy

server = {} # Filled in from command line
db_url = "" # Filled in from command line

def jobman_status_string( i ):
    d = {jobman.sql.START: 'QUEUED', jobman.sql.RUNNING: 'RUNNING',
         jobman.sql.DONE: 'DONE',   jobman.sql.ERR_START: 'ERR_START',
         jobman.sql.ERR_SYNC: 'ERR_SYNC', jobman.sql.ERR_RUN: 'ERR_RUN',
         jobman.sql.CANCELED: 'CANCELED'}
    if i in d.keys():
        return d[i]
    else:
        return str(i)

# From https://github.com/liudmil-mitev/experiments/blob/master/time/humanize_time.py
INTERVALS = [1, 60, 3600, 86400, 604800, 2419200, 29030400]
NAMES = [('second', 'seconds'),
         ('minute', 'minutes'),
         ('hour',   'hours'),
         ('day',    'days'),
         ('week',   'weeks'),
         ('month',  'months'),
         ('year',   'years')]
def humanize_time(amount, units):
   '''
      Divide `amount` in time periods.
      Useful for making time intervals more human readable.

      >>> humanize_time(173, "hours")
      [(1, 'week'), (5, 'hours')]
      >>> humanize_time(17313, "seconds")
      [(4, 'hours'), (48, 'minutes'), (33, 'seconds')]
      >>> humanize_time(90, "weeks")
      [(1, 'year'), (10, 'months'), (2, 'weeks')]
      >>> humanize_time(42, "months")
      [(3, 'years'), (6, 'months')]
      >>> humanize_time(500, "days")
      [(1, 'year'), (5, 'months'), (3, 'weeks'), (3, 'days')]
   '''
   result = []

   unit = map(lambda a: a[1], NAMES).index(units)
   # Convert to seconds
   amount = amount * INTERVALS[unit]

   for i in range(len(NAMES)-1, -1, -1):
      a = amount // INTERVALS[i]
      if a > 0: 
         result.append( (a, NAMES[i][1 % a]) )
         amount -= a * INTERVALS[i]

   return result

class JobmanMonitorServer(SimpleHTTPServer.SimpleHTTPRequestHandler):    
    def do_GET(self):
        path = urlparse.urlparse(self.path).path
        args = urlparse.parse_qs(urlparse.urlparse(self.path).query)
        if path=='/':
            commands = filter( lambda x: x[0:3]=="do_" and x!="do_HEAD" and x!="do_GET", dir(self) )
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'html')
            self.end_headers()
            self.wfile.write( "Available commands: " )
            map( lambda x: self.wfile.write(x[3:] + " "), commands )
        else:
            command = "do_" + path[1:]
            if command in dir(self):
                func = getattr( self, command )
                try:
                    func(args)
                except:
                    self.send_python_error()
            else:
                self.send_error(404, "File not found")
                return None

    def do_delete_experiment( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. delete_experiment?experimentid=0')
            return
        eid = int(args['experimentid'][0])
        
        cur = self.get_cursor()
        global server
        query1 = "delete from %skeyval where dict_id=%d;"%( server['tablename'], eid )
        query2 = "delete from %strial where id=%d;"%( server['tablename'], eid )
        cur.execute(query1 )
        self.conn.commit()
        self.wfile.write( "Deleted %d rows\n"%cur.rowcount )
        cur.execute(query2 )
        self.conn.commit()
        self.wfile.write( "Deleted %d rows"%cur.rowcount )       
    
    def do_reschedule_experiment( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. reschedule_experiment?experimentid=0')
            return
        eid = map( lambda x: int(x), args['experimentid'][0].split(",") )
        if 'force' in args.keys():
            force = args['force'][0]
            if force=='true':
                force = True
            else:
                force = False
        else:
            force = False
        
        cur = self.get_cursor()
        idcond = "id=" + " or id=".join( map(lambda x: str(x), eid ) )
        cur.execute( "select status from %strial where %s;"%(server['tablename'],idcond) )
        rows = cur.fetchall()
        if len(rows)==0:
            self.wfile.write("No job with ID %d"%str(eid))
            return
        if int(rows[0][0])==jobman.sql.START:
            self.wfile.write("Job with ID %d is already queued"%eid[0])
            return
        if int(rows[0][0])==jobman.sql.RUNNING and (not force):
            self.wfile.write("Job with ID %d is running. Are you sure you want to restart? If so ?force=true "%eid[0])
            return
        
        query1 = "update %strial set status=%d where %s;"%(server['tablename'],jobman.sql.START,idcond)
        cur.execute( query1 )
        self.conn.commit()
        self.wfile.write( "Updated %d rows\n"%cur.rowcount )
        idconddict = "dict_id=" + " or dict_id=".join( map(lambda x: str(x), eid ) )
        query2 = "update %skeyval set ival=%d where %s and name='jobman.status';"%(server['tablename'],jobman.sql.START,idconddict)
        cur.execute( query2 )
        self.conn.commit()
        self.wfile.write( "Updated %d rows"%cur.rowcount )

    def do_experiment_yaml_template( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        if not 'experimentid' in args.keys():
            self.wfile.write('Need to supply job id, e.g. experiment_yaml_template?experimentid=0')
        eid = int(args['experimentid'][0])
        
        cur = self.get_cursor()
        cur.execute( "select yamltemplate from " + server['view'] + " where id=" + str(eid) + ";")
        rows = cur.fetchall()
        if len(rows)==0:
            self.wfile.write('The view does not have a job with id ' + str(eid))
            return
        yaml = rows[0][0]
        self.wfile.write(yaml)
            
    #~ def get_cursor( self ):
        #~ if not hasattr(self, 'conn' ):
            #~ try:
                #~ self.conn = psycopg2.connect("dbname='" + server['dbname'] +
                                        #~ "' user='" + server['user'] +
                                        #~ "' host='" + server['host'] + 
                                        #~ "' password='"+ server['password'] + "'")
            #~ except:
                #~ self.send_python_error()
                #~ raise Exception("Could not connect to database")
        #~ if not hasattr( self, 'cursor' ):
            #~ self.cursor = self.conn.cursor()
        #~ return self.cursor
    
    def get_session( self ):
        global db_url
        if not hasattr(self, 'db' ):
            self.db = None
        if self.db==None:
            self.db = jobman.api0.open_db(db_url, serial=True)        
        if not hasattr(self, 'session' ):
            self.session = None
        if self.session==None:
            self.session = self.db.session()
        return self.session
    
    def get_dcts( self ):
        if not hasattr(self, 'dcts' ):
            s = self.get_session()
            q = s.query(self.db._Dict)
            q = q.options(sqlalchemy.orm.eagerload('_attrs')) #hard-coded in api0        
            self.dcts = q.all()
        return self.dcts
    
    def commit(self):
        self.session.commit()

    experiment_graph_template = """
                        <html>
                        <body>                         
                        <form action="/experiment_graph" method="GET">
                            <input type="hidden" name="id" value="{{ id }}" /> 
                            {% for checkbox in checkboxes %}
                                {% if checkbox in keys %}
                                    <input type="checkbox" name="key" value="{{ checkbox }}" checked>{{ checkbox }}<br>
                                {% else %}
                                    <input type="checkbox" name="key" value="{{ checkbox }}">{{ checkbox }}<br>
                                {% endif %}
                            {% endfor %}
                            <input type="submit"> 
                        </form>
                        <img src="/render_graph?id={{ id }}{% for key in keys %}&key={{ key }}{% endfor %}\"/>
                        </body>
                        </html>"""

        
    def do_experiment_graph( self, args ):
        id = int(args['id'][0])
        keys = args['key']
        dcts = self.get_dcts()
        checkboxes = []
        for dct in dcts:
            if dct.id==id:
                for key, val in dct.items():
                    if isinstance(val,list):
                        if isinstance(val[0], float) or isinstance(val[0], int):
                            checkboxes.append( key )

        
        env = jinja2.Environment( loader=jinja2.DictLoader( {'output':  self.experiment_graph_template } ) )
        self.wfile.write( env.get_template('output').render(checkboxes = checkboxes, id=id, keys=keys ) )
                                    
        
    
    def do_render_graph( self, args ):
        id = map( lambda x: int(x), args['id'] )        
        keys = args['key']
        print args
        if 'thumb' in args:
            if args['thumb'][0]=='True':
                thumb = True
            else:
                thumb = False
        else:
            thumb = False
            
        if 'from_epoch' in args.keys():
            from_epoch = int(args['from_epoch'][0])
        else:
            from_epoch = 0
        #graph_spec = args['graph_spec'][0]
        #graph_spec=urllib.unquote_plus( graph_spec ) 
        
        #cur = self.get_cursor()        
        #cur.execute( "select results_%s from %s where id=%d;"%(colname, server['view'], eid) )
        #rows = cur.fetchall()
        dcts = self.get_dcts()
        for dct in dcts:
            if dct.id in id:
                #if graph_spec[0]=='graph':
                #    yaxis = graph_spec[1]
                #    xaxis = graph_spec[2]
                #    curves = graph_spec[3]
                #else:
                #    xaxis = 'bla'
                #    yaxis = 'bla'
                #    curves = {'bla': graph_spec}
                curves = {}
                for key in keys:
                    curves["id " + key] = dct[key]
        
        plt.figure()
        #plt.xlabel( xaxis )
        #plt.ylabel( yaxis )
        for label,curve in curves.items():
            plt.plot( curve, label=label )
        plt.ylim( [from_epoch, max( map( lambda x: len(x), curves.values() ) )] )
        plt.legend()
        if thumb:
            plt.savefig(self.wfile, format = 'png', dpi=(25))
        else:
            plt.savefig(self.wfile, format = 'png' )
    
    monitor_template = """
                   <html>
                     <body>
                       <link rel="stylesheet" type="text/css" href="//cdn.datatables.net/1.10.0-beta.1/css/jquery.dataTables.css">
                       <script type="text/javascript" language="javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
                       <script type="text/javascript" language="javascript" src="http://cdn.datatables.net/1.10.0/js/jquery.dataTables.js"></script>
                       <LINK href="http://cdn.datatables.net/1.10.0/css/jquery.dataTables.css" rel="stylesheet" type="text/css">
                       <script type="text/javascript" language="javascript" src="http://cdn.datatables.net/colreorder/1.1.1/js/dataTables.colReorder.min.js"></script>
                       <LINK href="http://cdn.datatables.net/colreorder/1.1.1/css/dataTables.colReorder.css" rel="stylesheet" type="text/css">
                       <script type="text/javascript" language="javascript" src="http://cdn.datatables.net/colvis/1.1.0/js/dataTables.colVis.min.js"></script>
                       <LINK href="http:////cdn.datatables.net/colvis/1.1.0/css/dataTables.colVis.css" rel="stylesheet"  type="text/css"> <!-- -->
                       <script>
                          $(document).ready(function() {
                             $('#example').dataTable( {
                                   /*dom: 'pRC',*/
                                   stateSave: true
                            });
                          } );
                       </script>

                       <center><h1>DB: {{ db_url }}</h1></center>
                       Missing information in the table? Try running jobman sqlview (see below)<br>
                       <table id="example" class="display" width="100%">
                          <thead>
                              <!--<tr>
                                  <td colspan=2><center><b>Control</b></center></td>
                                  <td colspan=9><center><b>Jobman data</b></center></td>
                                  <td colspan={{ hyperparam_columns|length }}><center><b>Hyperparams</b></center></td>
                                  <td colspan={{ result_columns|length }}><center><b>Results</b></center></td>
                              </tr>-->
                              <tr>
                                  <td><b>Ctl</b></td>
                                  <td><b>ID</b></td>
                                  {% for col in cols %}
                                     <td><b>{{ col }}</b></td>
                                  {% endfor %}
                              </tr>
                          </thead>
                          <tbody>
                              {% for row in rows %}
                                 <tr>
                                    <td><span title="x=delete o=reschedule"><a href="/delete_experiment?experimentid={{ row[0] }}">x</a>&nbsp;<a href="/reschedule_experiment?experimentid={{ row[0] }}">o</a></span></td>                                 
                                    <td>{{ row[0] }}</td>
                                    {% for col in cols %}
                                        <td>{{ row[1][col] }}</td>
                                    {% endfor %}
                                 </tr>
                              {% endfor %}
                            </tbody>
                        </table>
                        <br>
                        To schedule a job:<br>
                        jobman -f sqlschedule {{ db_url }} experiment.train_experiment [conf file]<br>
                        To run a job:<br>
                        jobman sql {{ db_url }} .<br>
                     </body>
                    </html>"""
    
    def do_get_key_contents( self, args ):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        
        id = int(args['id'][0])
        key = args['key'][0]
        dcts = self.get_dcts()
        for dct in dcts:
            if dct.id==id:
                self.wfile.write( dct[key] )
    
    def columns( self, dcts ):
        cols = {}
        for dct in dcts:
            for k in dct.keys():
                cols[k] = ''
        ret = cols.keys()
        ret.sort()
        return ret

    def do_monitor(self, args):
        global db_url
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()    
        
        dcts = self.get_dcts()
        cols = self.columns( dcts )
        
        print jobman.tools.expand( dcts[0] )
        
        rows = []
        for dct in dcts:
            rowcols = {}
            for col in cols:
                if col in dct.keys():
                    #if isinstance(dct[col],list):
                    s = str(dct[col])
                    if len(s)>20:
                        if len(s)>100:
                            s = "<span title=\"%s\"><a href=\"get_key_contents?id=%d&key=%s\">%s...</a></span>"""%( s, dct.id, col, s[:20] )
                        else:
                            s = "<span title=\"%s\">%s...</span>"""%( s, s[:20] )
                    rowcols[col] = s
                else:
                    rowcols[col] = None                    
            rows.append( (dct.id, rowcols) )
        
        env = jinja2.Environment( loader=jinja2.DictLoader( {'output':  self.monitor_template } ) )
        self.wfile.write( env.get_template('output').render(cols = cols, rows=rows) )
        
        #self.wfile.write( self.columns( dcts ) ) 

        #for d in dcts:
        #    self.wfile.write("A dict\n")            
        #    self.wfile.write(str(dir(d)) + "\n")
        #    self.wfile.write(str(d.id) + "\n")
        #    for k in d.keys():
        #        self.wfile.write(k + "\n")
    
    def do_monitor_(self, args):
        global server
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        
        colstoget = ["id", "jobman_status", "jobman_sql_hostname", "jobman_sql_hostworkdir", "jobman_starttime", "jobman_endtime", "jobman_runtime", "jobman_lastsavedtime"]
        colnames = self.get_column_names()
        
        result_columns = filter( lambda x: x.startswith("results_"), colnames )
        hyperparam_columns = filter( lambda x: x.startswith("hyperparameters_"), colnames )
        
        colstoget = colstoget + result_columns + hyperparam_columns
        result_columns = map( lambda x: x[len("results_"):], result_columns )
        hyperparam_columns = map( lambda x: x[len("hyperparameters_"):], hyperparam_columns )
                
        cur = self.get_cursor()
        query = "select " + ",".join(filter( lambda x: x in colnames, colstoget )) + " from " + server['view'] + " order by id;"

        cur.execute( query )
        rows = cur.fetchall()
        for i in range(len(rows)):
            rows[i] = list(rows[i])
            for j in range(len(colstoget)):
                if not colstoget[j] in colnames:
                    rows[i].insert(j, None )
        for i in range(len(rows)):
            rows[i][1] = jobman_status_string( rows[i][1] ) # status
            if rows[i][4]!=None: # start time
                rows[i][4] = datetime.datetime.fromtimestamp(float(rows[i][4])).strftime('%Y-%m-%d %H:%M:%S')
            if rows[i][5]!=None: # end time
                rows[i][5] = datetime.datetime.fromtimestamp(float(rows[i][5])).strftime('%Y-%m-%d %H:%M:%S')
            if rows[i][6]!=None: # run time
                rows[i][6] = " ".join( map( lambda unit: " ".join( map( lambda x: str(x), unit ) ), humanize_time(int(rows[i][6]), "seconds") ) )
            if rows[i][7]!=None: # last update time
                rows[i][7] = " ".join( map( lambda unit: " ".join( map( lambda x: str(x), unit ) ), humanize_time(int(time.time()-float(rows[i][7])), "seconds") ) ) + " ago"
                if rows[i][0]!='RUNNING': # Don't display last update time if experiment has ended
                    rows[i][7] = None
            therest = rows[i][8:]
            if len(therest)>0:
                rows[i][8] = {} # results
                rows[i][9] = {} # hparams
                for j,col in enumerate(result_columns):
                    try:
                        rows[i][8][col] = json.loads(therest[j])
                    except:
                        rows[i][8][col] =  therest[j]
                    if isinstance( rows[i][8][col], list):
                        if rows[i][8][col][0]=='graph' or isinstance( rows[i][8][col][0], float ) or isinstance( rows[i][8][col][0], int ):
                            #imgurl = "/render_graph?graph_spec=%s"%(urllib.quote_plus( results[j] ))
                            imgurl = "/render_graph?experimentid=%d&colname=%s"%(rows[i][0],col)
                            scale = "10%"
                            rows[i][8][col] = "<a href=\"%s\"><img height=\"%s\" src=\"%s\"></a>"%(imgurl,scale,imgurl)
                        elif rows[i][8][col][0]=='sound':
                            rows[i][8][col] = 'sound'
                        elif isinstance( rows[i][8][col][0], list ):
                            rows[i][8][col] = "list of lists"
                for j,col in enumerate(hyperparam_columns):
                    rows[i][9][col] = therest[len(result_columns) + j]
        
        env = jinja2.Environment( loader=jinja2.DictLoader( {'output':  self.template} ) )
        self.wfile.write( env.get_template('output').render(rows=rows, result_columns=result_columns, db_url=db_url, hyperparam_columns=hyperparam_columns) )
    
    def get_column_names( self ):
        cur = self.get_cursor()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='%s'"%server['view'])
        return map( lambda x: x[0], cur.fetchall() )
        
#    def send_ascii_encoded_array( self, arr ):
#        self.send_response(200, 'OK')
#        self.send_header('Content-type', 'html')
#        self.end_headers()
#        ascii = base64.b64encode( cPickle.dumps( arr ) )
#        self.wfile.write( ascii )
#
    def send_python_error(self):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        self.wfile.write( traceback.format_exc() )
        print traceback.format_exc()


def start_web_server( wait = True ):
    def web_server_thread():
        httpd = None
        for port in range(8000, 8010):
            server_address = ('', port)
            try:
                httpd = BaseHTTPServer.HTTPServer(server_address, JobmanMonitorServer)
                break
            except:
                print "Could not start on port",port,", trying next"
        assert httpd!=None
        sa = httpd.socket.getsockname()
        print "Serving HTTP on", sa[0], "port", sa[1], "..."
        httpd.serve_forever()

    t = threading.Thread( target = web_server_thread) #, args = (self,) )
    t.daemon = True
    t.start()
    if wait:
        try:
            while True:
                time.sleep( 10000 )
        except KeyboardInterrupt:
            return

if __name__ == "__main__":
    try:
        _,db_url = sys.argv
    except:
        print "Need db url argument (postgres://user:password@host/db_name?table=table_name)"
    else:
        if len(sys.argv)<2:
            print "Need arguments host user password dbname tablename [view]"
            print "(View is optional, if omitted view is set to [tablename]view"
        else:
            start_web_server()
