import MySQLdb
	
# this file will be checked by a script.
# make sure you dont change variable names like ans1, or q1
# make sure your database name is amazon
# and your tables are called links and metadata

# write your UNI
uni = "zx2201"

# after you're done, write down the answers below
ans1 = "Fodor's Australia 2000"
ans2 = "My Louisiana Sky"
ans3 = 4.7113
ans4 = 10
ans5 = 9
ans6 = 717719

#ans6 comes from the Spark part

# SQL code begins
# connect to MySQL server
db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="Bill_xzf940207",  # your password
                     db="amazon")        # name of the data base

# The cursor object is your handle to the DB. 
# Use it to execute queries
cur = db.cursor()

def example():
	"""
	Example query to get the ID of our cat book
	"""
	# Use all the SQL you like
	cat_book = '"The Maine Coon Cat (Learning About Cats)"'
	cur.execute("SELECT * FROM metadata WHERE title="+cat_book)

	row = cur.fetchone()
	return row[1]

def q1():
	"""
	Code for q1 goes here
	"""
	cur.execute("SELECT to_item, COUNT(1) as count FROM links GROUP BY to_item ORDER BY count DESC LIMIT 1")
	row = cur.fetchone()
	item_id = row[0]
	cur.execute("SELECT * FROM metadata WHERE id ="+str(item_id))
	row = cur.fetchone()
	ans1 = row[1]
	return ans1

def q2():
	"""
	Code for q2 goes here
	"""
	cur.execute("SELECT links.to_item, COUNT(1) as count FROM links, metadata WHERE links.to_item = metadata.id AND metadata.type = 'dvd' GROUP BY to_item ORDER BY count DESC LIMIT 1")
	row = cur.fetchone()
	item_id = row[0]
	cur.execute("SELECT * FROM metadata WHERE id ="+str(item_id))
	row = cur.fetchone()
	ans2 = row[1]
	return ans2

def q3():
	"""
	Code for q3 goes here
	"""
	cur.execute("SELECT AVG(count) FROM (SELECT to_item, COUNT(1) as count FROM links GROUP BY to_item ORDER BY count DESC) as counts")
	row = cur.fetchone()
	ans3 = row[0]
	return ans3

def q4():
	"""
	Code for q4 goes here
	"""
	cat_book = '"The Maine Coon Cat (Learning About Cats)"'
	cur.execute("SELECT id FROM metadata WHERE title =" + cat_book)
	row = cur.fetchone()
	cat_id = row[0]
	cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS from_table AS (SELECT from_item FROM links WHERE to_item =" + str(cat_id) + ")")
	cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS to_table AS (SELECT to_item FROM links WHERE from_item =" + str(cat_id) + ")")
	cur.execute("SELECT COUNT(1) FROM links WHERE to_item IN (SELECT * FROM from_table) AND from_item IN (SELECT * FROM to_table)")
	row = cur.fetchone()
	ans5 = row[0]
	return ans5

def q5():
	"""
	Code for q5 goes here
	"""
	from_node = '"Star Wars Animated Classics - Droids"'
	to_node = '"The Maine Coon Cat (Learning About Cats)"'
	cur.execute("SELECT id FROM metadata WHERE title =" + from_node)
	row = cur.fetchone()
	from_id = row[0]
	cur.execute("SELECT id FROM metadata WHERE title =" + to_node)
	row = cur.fetchone()
	to_id = row[0]
	
	cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temp_0 AS (SELECT to_item FROM links WHERE from_item =" + str(from_id) + ")")
	cur.execute("SELECT * FROM temp_0 WHERE to_item =" + str(to_id))
	row = cur.fetchone()
	if row is None: is_contain = False
	else: is_contain = True
	i = 0
	while is_contain is False:
		cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temp_" + str(i+1) +  " AS (SELECT DISTINCT t2.to_item FROM temp_" + str(i) + " t1 LEFT JOIN links t2 ON t1.to_item = t2.from_item)")
		cur.execute("SELECT * FROM temp_" + str(i+1) + " WHERE to_item =" + str(to_id))
		row = cur.fetchone()
		if row is None: is_contain = False
		else: is_contain = True
		i += 1
	ans5 = i + 1
	return ans5


ans0 = example()
print ans0

ans1 = q1()
print ans1

ans2 = q2()
print ans2

ans3 = q3()
print ans3

ans4 = q4()
print ans4

ans5 = q5()
print ans5

db.close()
