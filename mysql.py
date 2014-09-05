#!/usr/bin/env python
#encoding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import time
from log.log import *

############################################
##  sql最大长度
############################################

class Mysql:
    def __init__(self, host="localhost", user="user",
                       password="passwd", database="dbname",
                       port=3306, charset="utf8", max_idle_time=7 * 3600,
                       wait_timeout=10 * 60, log=None):
        self.host     = host
        self.user     = user
        self.passwd   = password
        self.database = database
        self.port     = int(port)
        self.charset  = charset
        self.logger   = log if log is not None else logger(verbose=False)
        self._last_use_time = time.time()
        self.wait_timeout   = int(wait_timeout)
        self.max_idle_time  = float(max_idle_time)

        try:
            Mysql.connect(self)
        except Exception, e:
            self.logger.log('Connect to MySQL Server Failed, host=%s,user=%s,\
             db=%s, port=%u, errmsg=%s' % (self.host, self.user,self.database,
              self.port, e))

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "conn", None) is not None:
            self.conn.close()
            self.conn = None

    def connect(self):
        self.close()
        self.conn = MySQLdb.connect(
            host    =   self.host,
            user    =   self.user,
            passwd  =   self.passwd,
            db      =   self.database,
            port    =   self.port,
            compress = 1,
            cursorclass = MySQLdb.cursors.DictCursor,
            charset =   self.charset
        )
        self.conn.autocommit(False)

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).

        # ping to ensure that the connection is alive.
        is_ping_blocked = False
        try:
            self.conn.ping()
        except :
            is_ping_blocked = True

        if (self.conn is None or
            is_ping_blocked is True or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self.connect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        # self.conn.cursor().execute('set wait_timeout = %d' % self.wait_timeout)
        return self.conn.cursor()

    def fetchall(self, sql, parameters=None):
        cursor = self._cursor()
        try:
            count = cursor.execute(sql, parameters)
            if count == 0:
                return [count, None]
            rows = cursor.fetchall()
            return [count, rows]
        finally:
            cursor.close()

    def fetch(self, sql, parameters=None):
        cursor = self._cursor()
        try:

            count = cursor.execute(sql)
            if count == 0:
                return None
            elif count > 1:
                raise Exception("Multiple rows returned for query")
            else:
                row = cursor.fetchone()
                return row
        finally:
            cursor.close()

    def execute(self, sql, parameters=None):
        cursor = self._cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.rowcount
        except Exception, e:
            errsql = sql % parameters
            self.logger.log("execute error, sql=[%s], errmsg=[%s]"\
                            % (errsql, e), "error")
        finally:
            cursor.close()

    def execute_lastrowid(self, sql, parameters=None):
        cursor = self._cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.lastrowid
        except Exception, e:
            errsql = sql % parameters
            self.logger.log("execute error, sql=[%s], errmsg=[%s]"\
                            % (errsql, e), "error")
        finally:
            cursor.close()

    def executemany(self, sql, parameters):
        cursor = self._cursor()
        try:
            cursor.executemany(sql, parameters)
            return cursor.rowcount
        except Exception, e:
            errsql = sql
            for values in parameters:
                errsql += (", " + str(values))
            self.logger.log("executemany error, sql=[%s], errmsg=[%s]"\
                % (errsql, e), "error")
        finally:
            cursor.close()

    def executemany_lastrowid(self, sql, parameters):
        cursor = self._cursor()
        try:
            cursor.executemany(sql, parameters)
            return cursor.lastrowid
        except Exception, e:
            errsql = sql
            for values in parameters:
                errsql += (", " + str(values))
            self.logger.log("executemany error, sql=[%s], errmsg=[%s]"\
                % (errsql, e), "error")
        finally:
            cursor.close()

    def set_autocommit():
        self.conn.autocommit(True)

    def commit(self):
        if self.conn is not None:
            try:
                self.conn.ping()
            except :
                self.connect()
            try:
                self.conn.commit()
            except Exception,e:
                self.conn.rollback()
                self.logger.log("can't commit", "error")

    def lock_row(self, table_name, condition):
        """
        id is the primary key
        """
        if not condition[condition.find('(') + 1 : condition.find(')')]:
            return
        try:
            self.execute("select * from %s where %s for update" % \
                (table_name,condition))
        except:
            self.logger.log("lock fails !", "error")


class MysqlPagingQuery(object):
    """
    docstring for MysqlPagingQuery
    the mysql table must include 'id' field
    """
    def __init__(self, mysql_db, num_per_page=100):
        super(MysqlPagingQuery, self).__init__()
        self.mysql_db = mysql_db
        self.num_per_page = int(num_per_page)
        self.flyacount = 0
        # match condition
        self.lastrowid = 0
        self.maxrowid = 1
        # without condition
        self.maxid_in_table = 0

    def __del__(self):
        print "The flyacount is ", self.flyacount

    def query(self, fields, table_name, condition=None):
        self.maxid_in_table = self.__get_maxid_in_table(table_name)
        if condition is not None:
            min_id = self.__get_min_id(table_name, condition)
            self.lastrowid = min_id
        else:
            self.maxrowid = self.maxid_in_table

        if 'id' not in fields.split(','):
            fields += ', id'
        while True:
            rows = self.__query_each_page(fields, table_name, condition)
            if rows is not None:
                yield rows
            else:
                break

    def __get_maxid_in_table(self, table_name):
        sql = r'select max(id) from %s' % table_name
        row = self.mysql_db.fetch(sql)
        return int(row['max(id)'])

    def __get_min_id(self, table_name, condition):
        sql = r'select min(id), max(id) from %s where %s' \
        % (table_name, condition)
        row = self.mysql_db.fetch(sql)
        if row['min(id)'] is None:
            return self.maxid_in_table

        self.maxrowid = int(row['max(id)'])
        return (int(row['min(id)']) - 1)

    def __update_lastrowid(self, row_datas):
        if row_datas[0] == 0:
            return
        id_list = [row['id'] for row in row_datas[1]]
        self.lastrowid = max(id_list)

    def __query_each_page(self, fields, table_name, condition):
        if self.lastrowid >= self.maxrowid:
            return None

        sql = ''
        if condition is not None:
            sql = r'select %s from %s where id > %s and %s order by id asc limit %s' % \
                    (fields, table_name, self.lastrowid, condition, \
                    self.num_per_page)
        else:
            sql = r'select %s from %s where id > %s limit %s' % \
                    (fields, table_name, self.lastrowid, self.num_per_page)
        print sql
        row_datas = self.mysql_db.fetchall(sql)
        self.flyacount = self.flyacount + row_datas[0]
        self.__update_lastrowid(row_datas)

        return row_datas[1]
