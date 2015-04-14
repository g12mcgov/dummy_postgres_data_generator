#
# Author: Grant McGovern
# Date: 7 April 2015
#
# Description: A simple script to insert dummy data into the GymPeak API PostgreSQL database.
#

import time
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

def insertAverageData(cursor, table):

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


def insertHistoricalData(cursor, table):

	queries = []

	activities = {0: "'exit'", 1: "'entry'"}

	weekdays = [
	"'monday'",
	"'tuesday'",
	"'wednesday'",
	"'thursday'"
	]

	# datetime.datetime.time(time_).seconds

	friday = "'friday'"
	saturday = "'saturday'"
	sunday = "'sunday'"


	#######################################
	##### First compute the weekdays ######
	#######################################

	# Closing time is equal to 11:30pm
	end_date = datetime.datetime(2015, 9, 15, 23, 30, 0)
	
	for day in weekdays:
		# Reset the counts after each day
		# Start at 10, to get higher occupancy stats
		count = 10

		# Opening time is equal to 6:00am
		start_time = datetime.datetime(2015, 9, 15, 5, 30, 0)
		
		while (start_time.time() < end_date.time()):

			activity = activities[randint(0, 1)]

			if activity == "'entry'":
				count += 1
			else:
				count -= 1

			if count < 0:
				count = 0

			query = """ INSERT INTO %s (activity, timestamp, count, day) VALUES (%s, %s, %i, %s); """ % (table, activity, "'" + start_time.strftime("%Y-%m-%d %H:%M:%S") + "'", count, day)	
			print query 
			
			queries.append(query)
			
			# Generate random seconds between 0 to 47 seconds
			random_number = randint(1, 47)

			# Increase the time each time by 1 hour
			start_time += datetime.timedelta(seconds=random_number)


	########################################
	###### Then compute Friday #############
	########################################

	# Opening time is equal to 5:30am
	start_time = datetime.datetime(2015, 9, 15, 5, 30, 0)
	# Closing time is equal to 11:30pm
	end_date = datetime.datetime(2015, 9, 15, 6, 0, 0)

	# Reset count
	count = 10

	day = "'friday'"

	while (start_time.time() < end_date.time()):

			activity = activities[randint(0, 1)]

			if activity == "'entry'":
				count += 1
			else:
				count -= 1

			if count < 0:
				count = 0

			query = """ INSERT INTO %s (activity, timestamp, count, day) VALUES (%s, %s, %i, %s); """ % (table, activity, "'" + start_time.strftime("%Y-%m-%d %H:%M:%S") + "'", count, day)
			print query 
			
			queries.append(query)
			
			# Generate random seconds between 0 to 47 seconds
			random_number = randint(1, 47)

			# Increase the time each time by 1 hour
			start_time += datetime.timedelta(seconds=random_number)


	########################################
	###### Then compute Saturday ###########
	########################################

	# Opening time is equal to 10:00am
	start_time = datetime.datetime(2015, 9, 15, 10, 0, 0)
	# Closing time is equal to 11:30pm
	end_date = datetime.datetime(2015, 9, 15, 7, 0, 0)

	# Reset count
	count = 10

	day = "'saturday'"

	while (start_time.time() < end_date.time()):

			activity = activities[randint(0, 1)]

			if activity == "'entry'":
				count += 1
			else:
				count -= 1

			if count < 0:
				count = 0

			query = """ INSERT INTO %s (activity, timestamp, count, day) VALUES (%s, %s, %i, %s); """ % (table, activity, "'" + start_time.strftime("%Y-%m-%d %H:%M:%S") + "'", count, day)	
			print query 
			
			queries.append(query)
			
			# Generate random seconds between 0 to 47 seconds
			random_number = randint(1, 47)

			# Increase the time each time by 1 hour
			start_time += datetime.timedelta(seconds=random_number)



	########################################
	###### Then compute Sunday #############
	########################################

	# Opening time is equal to 1:00pm
	start_time = datetime.datetime(2015, 9, 15, 13, 0, 0)
	# Closing time is equal to 11:30pm
	end_date = datetime.datetime(2015, 9, 15, 23, 30, 0)


	# Reset count
	count = 10

	day = "'sunday'"

	while (start_time.time() < end_date.time()):

			activity = activities[randint(0, 1)]

			if activity == "'entry'":
				count += 1
			else:
				count -= 1

			if count < 0:
				count = 0

			query = """ INSERT INTO %s (activity, timestamp, count, day) VALUES (%s, %s, %i, %s); """ % (table, activity, "'" + start_time.strftime("%Y-%m-%d %H:%M:%S") + "'", count, day)
	
			print query 
			
			queries.append(query)
			
			# Generate random seconds between 0 to 17 seconds
			random_number = randint(1, 17)

			# Increase the time each time by 1 hour
			start_time += datetime.timedelta(seconds=random_number)


	insertionCount = 0
	# Insert queries into database
	print "\nInserting queries... (This might take awhile)."
	for query in queries:
		try:
			cursor.execute(query)
			insertionCount += 1
		except psycopg2.Error as err:
			raise err

	print "Query Ok.", "Inserted (%i) Rows." % insertionCount

	return queries

			
def writeToFile(queries, name):
	outfile = open(name,'w')

	for line in queries:
		outfile.write(line)
		outfile.write("\n")
	
	outfile.close()


if __name__ == "__main__":

	host = "ec2-54-163-226-9.compute-1.amazonaws.com"
	database = "d88utg4hgqpgob"
	username = "uppsdlfesoxmrt"
	password = "ESZMChU9TsTXJmvEDBvXtm4Vht"

	# Generate a cursor
	connection = establishConnection(host, database, username, password)
	cursor = connection.cursor()


	schools_average = [
	'"unc.average"',
	'"davidson.average"'
	]

	schools_historical = [
	'"davidson.historical"'
	]

	#response = queryTable(cursor, '"wakeforest.average"', "'monday'")
	# for school in schools_average:
	# 	insertAverageData(cursor, school)

	# for school in schools_historical:
	# 	queries = insertHistoricalData(cursor, school)

	connection.commit()

	writeToFile(queries, "historical.sql")
