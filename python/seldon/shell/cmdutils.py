import MySQLdb as mdb
import json
import re

def get_conn(user, passwd, db, host, port):
    conn = mdb.connect(user=user, passwd=passwd,db=db, host=host, port=port)
    return conn;

def get_db_details(zk_client):
    db_details = {}
    dbcp_node =  zk_client.get('/config/dbcp')
    dbcp_details_json = dbcp_node[0]
    dbcp_details = json.loads(dbcp_details_json)
    for db_info in dbcp_details['dbs']:
        name = db_info['name']
        user = db_info['user']
        passwd = db_info['password']
        db = 'mysql'
        jdbc = db_info['jdbc']
        m=re.search(':\/\/(.*)[:](.*),', jdbc)
        host = m.group(1)
        port = int(m.group(2))
        db_details[ name ] = {
            "host" : host,
            "port" : port,
            "status" : "",
            "error_msg" : ""
        }
        try:
            conn = get_conn(user, passwd, db, host, port)
            try:
                cur = conn.cursor(mdb.cursors.DictCursor)
                sql="show status like 'Threads_connected%';"
                cur.execute(sql)
                rows = cur.fetchall()
                cur.close()
                db_details[ name ]['status'] = "OK"
            except Exception as e:
                error_msg=str(e)
                db_details[ name ]['status'] = "FAILED"
                db_details[ name ]['error_msg'] = error_msg
            finally:
                conn.close()
        except Exception as e:
            error_msg=str(e)
            db_details[ name ]['status'] = "FAILED"
            db_details[ name ]['error_msg'] = error_msg
    return db_details
