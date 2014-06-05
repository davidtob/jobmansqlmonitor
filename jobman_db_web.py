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
   

def tupleify( l ):
    if isinstance( l, list ) or isinstance( l, tuple ):
        return tuple( map( tupleify, l ) )
    else:
        return l

# Is caching really necessary? Seems not to give such a big improvement in load speeds
class Grapher(object):
    def __init__(self):
        self.cache = {}
    
    def render_graph( self, curves, thumb, from_epoch = 0 ):
        curves = tupleify(curves)
        #print "Getting ", curves
        #print self.cache.keys()   
        if (curves,thumb) in self.cache.keys():            
            return self.cache[ (curves,thumb) ]
        else:
            plt.figure()
            #plt.xlabel( xaxis )
            #plt.ylabel( yaxis )
            for label,curve in curves:
                plt.plot( curve, label=label )
            plt.xlim( [from_epoch, max( map( lambda x: len(x), map( lambda y: y[1], curves) ) )] )
            plt.legend()
            buf = io.BytesIO()    
            if thumb:
                plt.savefig(buf, format = 'png', dpi=(15))
            else:
                plt.savefig(buf, format = 'png' )        
            buf.seek(0)
            #print "A", self.cache.keys()
            self.cache[ (curves,thumb) ] = buf.read()
            #print "B", self.cache.keys()
            buf.close()
            return self.cache[ (curves,thumb) ]
    
jobman_monitor_server = None

class JobmanMonitorRequest(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):   
        path = urlparse.urlparse(self.path).path
        args = urlparse.parse_qs(urlparse.urlparse(self.path).query)
        if path=='/':
            commands = filter( lambda x: x[0:3]=="do_" and x!="do_HEAD" and x!="do_GET", dir(jobman_monitor_server) )
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'html')
            self.end_headers()
            self.wfile.write( "Available commands: " )
            map( lambda x: self.wfile.write(x[3:] + " "), commands )
        else:
            command = "do_" + path[1:]
            global jobman_monitor_server
            if command in dir(jobman_monitor_server):
                func = getattr( jobman_monitor_server, command )
                try:                    
                    func(self.wfile, args)
                except:
                    self.send_python_error()                
            else:
                self.send_error(404, "File not found")
                return None

    def send_python_error(self):
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'html')
        self.end_headers()
        self.wfile.write( traceback.format_exc() )
        print traceback.format_exc()
                

class JobmanMonitorServer(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def __init__(self, db_url):
        self.grapher = Grapher()
        self.db_url = db_url
        print "Connecting to db"
        self.db = jobman.api0.open_db(self.db_url, serial=True)
        print "done"
        
    def do_delete_experiment( self, wfile, args ):
        if not 'id' in args.keys():
            wfile.write( "Need to supply job id, e.g. delete_experiment?id=0'" )
            return
        id = map( lambda x: int(x), args['id'] )
        
        dcts = self.get_dcts()
        s = self.get_session()
        n = 0
        for dct in dcts:
            if dct.id in id:
                dct.delete( s )
                n = n + 1
        
        s.commit()             
        wfile.write( "Deleted %d jobs"%n )        
        #cur = self.get_cursor()
        #global server
        #query1 = "delete from %skeyval where dict_id=%d;"%( server['tablename'], eid )
        #query2 = "delete from %strial where id=%d;"%( server['tablename'], eid )
        #cur.execute(query1 )
        #self.conn.commit()
        #wfile.write( "Deleted %d rows\n"%cur.rowcount )
        #cur.execute(query2 )
        #self.conn.commit()
        #wfile.write( "Deleted %d rows"%cur.rowcount )       
    
    def do_reschedule_experiment( self, wfile, args ):
        if not 'id' in args.keys():
            self.wfile.write('Need to supply job id, e.g. reschedule_experiment?experimentid=0')
            return
        id = map( lambda x: int(x), args['id'] )
        if 'force' in args.keys():
            force = args['force'][0]
            if force=='true':
                force = True
            else:
                force = False
        else:
            force = False
        
        dcts = self.get_dcts()
        for dct in dcts:
            thisid = dct.id
            if dct.id in id:
                if dct["jobman.status"]==jobman.sql.START:
                    wfile.write("Job with ID %d is already queued\n"%dct.id)
                elif dct["jobman.status"]==jobman.sql.RUNNING and (not force):
                    wfile.write("Job with ID %d is running. Are you sure you want to restart? If so ?force=true\n"%dct.id)
                else:
                    #s = self.get_session()
                    #dct._set_in_session( "jobman.status", jobman.sql.START, session = s )
                    #s.commit()
                    dct["jobman.status"] = jobman.sql.START
                    dct.refresh()
                    wfile.write("Job with ID %d is rescheduled\n"%thisid)
                    
        #~ 
        #~ query1 = "update %strial set status=%d where %s;"%(server['tablename'],jobman.sql.START,idcond)
        #~ cur.execute( query1 )
        #~ self.conn.commit()
        #~ self.wfile.write( "Updated %d rows\n"%cur.rowcount )
        #~ idconddict = "dict_id=" + " or dict_id=".join( map(lambda x: str(x), eid ) )
        #~ query2 = "update %skeyval set ival=%d where %s and name='jobman.status';"%(server['tablename'],jobman.sql.START,idconddict)
        #~ cur.execute( query2 )
        #~ self.conn.commit()
        #~ wfile.write( "Updated %d rows"%cur.rowcount )
    
    def get_dcts( self ):
        s = self.db.session()
        q = s.query(self.db._Dict)
        q = q.options(sqlalchemy.orm.eagerload('_attrs')) #hard-coded in api0        
        dcts = q.all()
        s.close()
        return dcts
    
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


    def get_numeric_list_columns( self, dcts ):        
        ret = {}
        all_cols = []
        for dct in dcts:
            thisid = []
            for key, val in dct.items():
                if isinstance(val,list):
                    if isinstance(val[0], float) or isinstance(val[0], int):
                        thisid.append( key )
            all_cols = all_cols + thisid
            ret[dct.id]= thisid
        print list(set(all_cols))
        return ret, list(set(all_cols))
        
    def do_experiment_graph( self, wfile, args ):
        id = int(args['id'][0])
        keys = args['key']
        dcts = self.get_dcts()
        checkboxes = self.get_numeric_list_columns( dcts )[0][id]
        
        env = jinja2.Environment( loader=jinja2.DictLoader( {'output':  self.experiment_graph_template } ) )
        wfile.write( env.get_template('output').render(checkboxes = checkboxes, id=id, keys=keys ) )

    def do_render_graph( self, wfile, args ):
        id = map( lambda x: int(x), args['id'] )
        keys = args['key']        
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
                curves = []
                for key in keys:
                    curves.append( ("id " + key,dct[key]) )
        
        #~ print hasattr(self, "grapher" )
        #~ if not hasattr(self, "grapher" ):
            #~ print "creating new grapher"
            #~ self.grapher = Grapher()
            #~ global asdasd
            #~ asdasd.append( self.grapher )
            #~ print asdasd
            #~ print hasattr(self, "grapher" )
        
        wfile.write( self.grapher.render_graph( curves, thumb, from_epoch ) )
    
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
                                  <td><b>Graph</b></td>
                                  {% for col in cols %}
                                     <td><b>{{ col }}</b></td>
                                  {% endfor %}
                              </tr>
                          </thead>
                          <tbody>
                              {% for row in rows %}
                                 <tr>
                                    <td><span title="x=delete o=reschedule"><a href="/delete_experiment?id={{ row[0] }}">x</a>&nbsp;<a href="/reschedule_experiment?id={{ row[0] }}">o</a></span></td>                                 
                                    <td>{{ row[0] }}</td>                                    
                                    <td>
                                    {% if checked_checkboxesperid[row[0]]|length > 0 %}
                                        <a href="/experiment_graph?id={{ row[0] }}{% for key in checked_checkboxesperid[row[0]] %}&key={{ key }}{% endfor %}">
                                            <img width="120" height="90" src="/render_graph?id={{ row[0] }}&thumb=True{% for key in checked_checkboxesperid[row[0]] %}&key={{ key }}{% endfor %}"></td>
                                        </a>
                                    {% endif %}
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
                        <form action="/monitor" method="GET">                            
                            {% for checkbox in allcheckboxes %}
                                {% if checkbox in checked_checkboxes %}
                                    <input type="checkbox" name="key" value="{{ checkbox }}" checked>{{ checkbox }}
                                {% else %}
                                    <input type="checkbox" name="key" value="{{ checkbox }}">{{ checkbox }}
                                {% endif %}
                            {% endfor %}
                            <input type="submit"> 
                        </form>
                     </body>
                    </html>"""
    
    def do_get_key_contents( self, wfile, args ):
        id = int(args['id'][0])
        key = args['key'][0]
        dcts = self.get_dcts()
        for dct in dcts:
            if dct.id==id:
                wfile.write( dct[key] )
    
    def columns( self, dcts ):
        cols = {}
        for dct in dcts:
            for k in dct.keys():
                cols[k] = ''
        ret = cols.keys()
        ret.sort()
        return ret

    def do_monitor(self, wfile, args):        
        if 'key' in args.keys():
            checked_checkboxes = args['key']
        else:
            checked_checkboxes = []
        
        dcts = self.get_dcts()
        cols = self.columns( dcts )
        
        checkboxesperid,allcheckboxes = self.get_numeric_list_columns( dcts )        
        
        checked_checkboxesperid = {}
        for k in checkboxesperid.keys():
            checked_checkboxesperid[k] = list(set(checkboxesperid[k]).intersection( checked_checkboxes ))
        
        rows = []
        for dct in dcts:
            rowcols = {}
            for col in cols:
                if col in dct.keys():
                    if col=="jobman.status":
                        print dct.id, jobman_status_string( dct["jobman.status"] )
                        rowcols[col] = jobman_status_string( dct["jobman.status"] )
                    elif col=="jobman.start_time" or col=="jobman.sql.start_time" or col=="jobman.last_update_time":
                        rowcols[col] = datetime.datetime.fromtimestamp(float(dct[col])).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        s = str( dct[col] )
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
        wfile.write( env.get_template('output').render(db_url = self.db_url, cols = cols, rows=rows, allcheckboxes=allcheckboxes, checked_checkboxes=checked_checkboxes, checked_checkboxesperid=checked_checkboxesperid ) )            
    
        #~ if rows[i][7]!=None: # last update time
            #~ rows[i][7] = " ".join( map( lambda unit: " ".join( map( lambda x: str(x), unit ) ), humanize_time(int(time.time()-float(rows[i][7])), "seconds") ) ) + " ago"
            #~ if rows[i][0]!='RUNNING': # Don't display last update time if experiment has ended
                #~ rows[i][7] = None

#    def send_ascii_encoded_array( self, arr ):
#        request.send_response(200, 'OK')
#        request.send_header('Content-type', 'html')
#        request.end_headers()
#        ascii = base64.b64encode( cPickle.dumps( arr ) )
#        self.wfile.write( ascii )
#

def start_web_server( wait = True ):
    def web_server_thread():
        httpd = None        
        for port in range(8000, 8010):
            server_address = ('', port)
            try:
                httpd = BaseHTTPServer.HTTPServer(server_address, JobmanMonitorRequest)
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
        jobman_monitor_server = JobmanMonitorServer( db_url )
        start_web_server()
