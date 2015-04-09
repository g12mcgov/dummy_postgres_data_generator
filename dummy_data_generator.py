#
# Author: Grant McGovern
# Date: 7 April 2015
#
# Description: A simple script to insert dummy data into the GymPeak API PostgreSQL database.
#


import psycopg2
import datetime
from random import randint	

def establishConnection(host, database, username, password):
	conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (host, database, username, password)

	print "Connecting to database...%s" % database
	try:
		connection = psycopg2.connect(conn_string)

		print "Connection Successful.\n"

		return connection

	except AttributeError as err:
		raise err

def queryTable(cursor, table, day):
	query = """ SELECT * FROM %s WHERE day=%s""" % (table, day)
	
	results = cursor.execute(query)

	return cursor.fetchall()

def insertDailyData(cursor, table):

	time = datetime.datetime(2015, 1, 1, 6, 0, 0)
	end_date = datetime.datetime(2015, 1, 1, 0, 0, 0)
	#difference = datetime.datetime.combine(datetime.date.today(), start_date) - datetime.datetime.combine(datetime.date.today(), end_date)

	weekdays = [
	"'monday'",
	"'tuesday'",
	"'wednesday'",
	"'thursday'"
	]

	friday = "'friday'"
	saturday = "'saturday'"
	sunday = "'sunday'"

	queries = []

	# First insert the weekdays 
	for day in weekdays:
		for i in range(0, 18):
			# Generate a random number between 30 and 85
			count = randint(30, 85)
			query = """ INSERT INTO %s (day, count, time) VALUES (%s, %i, %s); """ % (table, day, count, "'" + time.strftime("%H:%M:%S") + "'")
			
			queries.append(query)

			# Increase the time each time by 1 hour
			time += datetime.timedelta(hours=1) 

	# Reset the time to 5:30am
	time = datetime.datetime(2015, 1, 1, 6, 0, 0)

	# Now generate query for Friday
	for i in range(0, 12):
		query = """ INSERT INTO %s (day, count, time) VALUES (%s, %i, %s); """ % (table, "'friday'", randint(30, 85), "'" + time.strftime("%H:%M:%S") + "'")
		
		queries.append(query)

		time += datetime.timedelta(hours=1)

	# Reset the time to 10:00am
	time = datetime.datetime(2015, 1, 1, 10, 0, 0)

	# Now generate queries for Saturday
	for i in range(0, 9):
		query = """ INSERT INTO %s (day, count, time) VALUES (%s, %i, %s); """ % (table, "'saturday'", randint(30, 85), "'" + time.strftime("%H:%M:%S") + "'")
		
		queries.append(query)

		time += datetime.timedelta(hours=1)


	# Reset the time to 1:00pm
	time = datetime.datetime(2015, 1, 1, 13, 0, 0)
	
	# Now generate queries for Sunday
	for i in range(0, 11):
		query = """ INSERT INTO %s (day, count, time) VALUES (%s, %i, %s); """ % (table, "'sunday'", randint(30, 85), "'" + time.strftime("%H:%M:%S") + "'")
		
		queries.append(query)

		time += datetime.timedelta(hours=1)

	results = []

	for query in queries:
		try:
			cursor.execute(query)
		except psycopg2.Error as err:
			raise err

	print "Query Ok."



if __name__ == "__main__":

	host = ""
	database = ""
	username = ""
	password = ""

	# Generate a cursor
	connection = establishConnection(host, database, username, password)
	cursor = connection.cursor()

	response = queryTable(cursor, '"wakeforest.average"', "'monday'")
	insertDailyData(cursor, '"wakeforest.average"')

	connection.commit()
