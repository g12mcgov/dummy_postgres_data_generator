#
# Author: Grant McGovern
#

import psycopg2

def connect(host, database, username, password):
	conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (host, database, username, password)

	print "Connecting to database...%s\n" % database
	try:
		connection = psycopg2.connect(conn_string)

		print "Connection Successful.\n"

		return connection.cursor()

	except AttributeError as err:
		raise err

def queryTable(cursor, table, day):
	query = 'SELECT * FROM %s WHERE day=%s' % (table, day)
	
	results = cursor.execute(query)

	print cursor.fetchall()

if __name__ == "__main__":
	host = "ec2-54-163-226-9.compute-1.amazonaws.com"
	database = "d88utg4hgqpgob"
	username = "uppsdlfesoxmrt"
	password = "ESZMChU9TsTXJmvEDBvXtm4Vht"

	cursor = connect(host, database, username, password)
	queryTable(cursor, '"wakeforest.average"', "'monday'")