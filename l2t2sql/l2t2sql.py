#!/usr/bin/python
# l2t2sql
# by Olivier Toelen (olivier.toelen@gmail.com)

import argparse
from getpass import getpass
import MySQLdb as db
import MySQLdb.constants.ER as ERR
import sys

# Show some activity on the screen while importing records:
def spinner(num):
	pick = num % 4	
	if pick == 0:
		img = '[-]'
	if pick == 1:
		img = '[\]'
	if pick == 2:
		img = '[|]'
	if pick == 3:
		img = '[/]'
	sys.stdout.write(img)
	sys.stdout.flush()
	sys.stdout.write('\b\b\b')		

def create_database(conn, database):
	cursor = conn.cursor()
	sql = "CREATE DATABASE %s" % database
	try:
		cursor.execute(sql)
	except db.Error, e:
		if e.args[0] != ERR.DB_CREATE_EXISTS:
			raise db.Error, e
	sql = "USE %s" % database
	cursor.execute(sql)
	cursor.close()

def create_table(conn, table):
	cursor = conn.cursor()
	sql = """
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			date DATE,
			time TIME,
			datetime DATETIME,
			timezone VARCHAR(63),
			macb VARCHAR(4),
			source VARCHAR(63),
			sourcetype VARCHAR(63),
			type VARCHAR(63),
			user TEXT,
			host VARCHAR(63),
			short TEXT,
			description TEXT,
			version INT UNSIGNED,
			filename TEXT,
			inode BIGINT UNSIGNED,
			notes TEXT,
			format VARCHAR(63),
			extra TEXT,
			identifier VARCHAR(255)
		)
	""" % table
	try:
		cursor.execute(sql)
	except db.Error, e:
		if e.args[0] != ERR.TABLE_EXISTS_ERROR:
			raise db.Error, e
	cursor.close()

def create_grants(conn, username, database, table):
	cursor = conn.cursor()
	# sql = 'GRANT SELECT,INSERT,DELETE ON %(database).%(table) TO %(username) localhost' % \
	sql = """
		GRANT SELECT,INSERT,DELETE 
		ON %(database)s.%(table)s 
		TO '%(username)s'@'localhost'
	""" % {"database": database,"table": table,"username": username}
	cursor.execute(sql)
	sql = """
		GRANT SELECT 
		ON %(database)s.%(table)s 
		TO '%(username)s'@'%%'
	""" % {"database": database,"table": table,"username": username}
	cursor.execute(sql)
	cursor.close()

def fill_table(conn, file, table, identifier):
	firstline = True
	cursor = conn.cursor()
	num_of_records = 0
	for line in file:
		if firstline:	# skip the header
			firstline = False
			continue
		part = line.split(',')
		# escape quotes:
                part = [ value.replace('\'', "\\'") for value in part ]
		num_of_records+=1
		spinner(num_of_records)
		date = "STR_TO_DATE('" + part[0] + "','%m/%d/%Y')"
		time = "STR_TO_DATE('" + part[1] + "','%k:%i:%s')"
		datetime = "STR_TO_DATE('" + part[0] + " " + part[1] + "','%m/%d/%Y %k:%i:%s')"
		sql = """
			INSERT INTO %(table)s
			(date, time, datetime, timezone, macb, source, sourcetype, type, user, host, short, description, version, filename, inode, notes, format, extra, identifier)
			VALUES(%(date)s,%(time)s,%(datetime)s,'%(timezone)s','%(macb)s','%(source)s','%(sourcetype)s','%(type)s','%(user)s','%(host)s','%(short)s','%(description)s',%(version)s,'%(filename)s',%(inode)s,'%(notes)s','%(format)s','%(extra)s','%(identifier)s')
    		""" % {"table":table,"date":date,"time":time,"datetime":datetime,"timezone":part[2],"macb":part[3],"source":part[4],"sourcetype":part[5],"type":part[6],"user":part[7],"host":part[8],"short":part[9],"description":part[10],"version":part[11],"filename":part[12],"inode":part[13],"notes":part[14],"format":part[15],"extra":part[16],"identifier":identifier}
		cursor.execute(sql)
	cursor.close()

### MAIN ###
if __name__ == "__main__":
	host = 'localhost'
	tablename = 'timeline'	
	parser = argparse.ArgumentParser(description='Imports a CSV file in l2t format to a local MySQL database',epilog='[olivier.toelen@gmail.com]')
	parser.add_argument('-u', '--user', metavar='USERNAME', help='the mysql user account (default:investigator)', default='investigator')
	parser.add_argument('-d', '--database', metavar='DATABASE', help='the mysql database name (default:cases)', default='cases')
	parser.add_argument('-i', '--identifier', metavar='IDENTIFIER', help='an identifier to identify this import with (default:TESTCASE)', default='TESTCASE')
	parser.add_argument('-s', '--setup', metavar='ADMINNAME', help='Setup local MySQL instance to support l2t2sql')
	parser.add_argument('filename', metavar='FILE', help='l2t output csv file')
	args = parser.parse_args()
	passwd = getpass("MySQL Password for %s: " % args.user)
	install = False
	if not args.setup is None:
		install = True
		admin_passwd = getpass("MySQL Password for db admin %s: " % args.setup)
	conn = None
	cursor = None
	try:
		if install:
			conn = db.connect(host,args.setup,admin_passwd) 
			create_database(conn, args.database)
			create_table(conn, tablename)
			create_grants(conn, args.user, args.database, tablename)
			conn.commit()
			print "> Local MySQL instance succesfully setup for l2t2sql."
			print "> To allow remote login for user '%s', make sure that:" % args.user
			print "\t - The local IP of this host is set as the bind-address in /etc/mysql/my.cnf"
			print "\t - The mysql service is restarted"
		conn = db.connect(host,args.user,passwd,args.database)
		print "> Filling table:"
		file = open(args.filename,'r')
		fill_table(conn, file, tablename, args.identifier)
		file.close()
		print ""
		conn.commit()
	except IOError, e:
		print "[File Error] %d: %s" % (e.args[0], e.args[1])
		exit(1)
	except db.Error, e:
		print "[Database Error] %d: %s" % (e.args[0], e.args[1])
	finally:
		if file:
			if not file.closed:
				file.close()
		if conn:
			if conn.open:
				conn.close()
		print "Done."
