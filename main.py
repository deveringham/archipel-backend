###
#
#       /\        /\\\\\\\         /\\    /\\     /\\ /\\ /\\\\\\\   /\\\\\\\\ /\\      
#      /\ \\      /\\    /\\    /\\   /\\ /\\     /\\ /\\ /\\    /\\ /\\       /\\      
#     /\  /\\     /\\    /\\   /\\        /\\     /\\ /\\ /\\    /\\ /\\       /\\      
#    /\\   /\\    /\ /\\       /\\        /\\\\\\ /\\ /\\ /\\\\\\\   /\\\\\\   /\\      
#   /\\\\\\ /\\   /\\  /\\     /\\        /\\     /\\ /\\ /\\        /\\       /\\      
#  /\\       /\\  /\\    /\\    /\\   /\\ /\\     /\\ /\\ /\\        /\\       /\\      
# /\\         /\\ /\\      /\\    /\\\\   /\\     /\\ /\\ /\\        /\\\\\\\\ /\\\\\\\\
#
###
# Dylan Everingham, Ben Levin
# 18.07.21
###
# Dependencies
###
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.exc import IntegrityError
from flask import Flask, request, Response, jsonify
import json

###
# Constants
###
DB_USER = 'root'
DB_PASS =  'Zee?Hond?'
DB_NAME = 'archipel'
DB_PORT = 3306

###
# Database access functions
###

# Add tag
def add_tag(connection, tag_name, lat, lon):
	try:
		q = text('''INSERT INTO tags(tag_name, lat, lon) 
			VALUES(:tag_name, :lat, :lon)''')
		connection.execute(q, tag_name=tag_name, lat=lat, lon=lon)
	except IntegrityError:
		return False
	return True



# Add message to tag (requires tag_name, msg)
def add_msg(connection, tag_name, msg):
	try:
		q = text('''INSERT INTO messages(tag_name, msg) 
			VALUES(:tag_name, :msg)''')
		connection.execute(q, tag_name=tag_name, msg=msg)
	except IntegrityError:
		return False
	return True

# Get all messages, location and timestamp from a tag
def get_tag(connection, tag_name):
	q = text('''SELECT lat, lon, msg, t.created_at tag_created_at, m.created_at msg_created_at 
		FROM tags t, messages m WHERE m.tag_name = t.tag_name 
		AND t.tag_name = :tag_name''')
	result = connection.execute(q, tag_name=tag_name).fetchall()
	print(result)
	if(len(result) < 1) : 
		return None
	lat, lon = result[0][0], result[0][1]
	created_at = str(result[0][3])
	messages = [ {'text': str(row[2]), 'created_at': str(row[4])} for row in result]
	return {'tag_name': tag_name, 'lat': lat, 'lon': lon, 'created_at': created_at, 
		'messages': messages}

# Get list of all tags, their locations and timestamps
def get_alltags(connection):
	result = connection.execute(
		text('SELECT tag_name, lat, lon, created_at FROM tags')).fetchall()
	tag_list = [{'tag_name': str(row[0]), 'lat': float(row[1]), 'lon': float(row[2]),
		'created_at': str(row[3])} for row in result]
	return tag_list

# Clear both tables
def clear_all(connection):
	connection.execute(text('DELETE FROM tags'))
	connection.execute(text('DELETE FROM messages'))
	return True


###
# Main routine
###

# Create sqlalchemy engine
engine = create_engine('mysql://' + DB_USER + 
	':' + DB_PASS + 
	'@localhost:' + str(DB_PORT) + 
	'/' + DB_NAME)
connection = engine.connect()

#with engine.connect() as connection:

# Select the archipel db
connection.execute(text('USE archipel'))

# Delete tables
connection.execute(text('DROP TABLE IF EXISTS messages'))
connection.execute(text('DROP TABLE IF EXISTS tags'))

# Initialize timezone for timestamp columns
connection.execute(text('SET time_zone = \'+00:00\''))

# Create tables, if they don't already exist
connection.execute(text('''CREATE TABLE IF NOT EXISTS tags 
	(tag_name VARCHAR(25) PRIMARY KEY,
	lat DOUBLE, lon DOUBLE,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'''))
connection.execute(text('''CREATE TABLE IF NOT EXISTS messages 
	(msg_id INT AUTO_INCREMENT PRIMARY KEY, 
	tag_name VARCHAR(25), 
	msg VARCHAR(255),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY (tag_name) REFERENCES tags(tag_name) ON DELETE CASCADE)'''))

# Clear the tables at startup
#clear_all(connection)

# Test stuff
add_tag(connection, 'test', 100, 200)
add_msg(connection, 'test', 'test_msg_0')
add_msg(connection, 'test', 'test_msg_1')
add_tag(connection, 'test', 300, 400)

print(get_alltags(connection))
print(get_tag(connection, 'test'))

##
# Flask app
##
app = Flask(__name__)
@app.route("/")
def hello_world():
    return '<p>Hello, World!</p>'

# Handle fetching a tag (GET) or creating a new one (POST)
@app.route('/tag/<tag_name>', methods=['GET', 'PUT'])
def handle_get_tag(tag_name):
	response = Response()
	response.headers.add('Access-Control-Allow-Origin', '*')
	if request.method == 'GET':
		result = get_tag(connection, tag_name)
		if result == None: 
			response.status_code = 404
			return response
		response = jsonify(result)
		response.headers.add('Access-Control-Allow-Origin', '*')
		return response
	else:
		result = add_tag(connection, tag_name)
		
		if result:
			response.status_code = 201
			return response
		else:
			response.status_code = 403
			return response

# Handle adding a new message to a tag
@app.route('/msg/<tag_name>', methods=['POST'])
def handle_add_msg(tag_name):
	result = add_msg(connection, tag_name, msg)
	response = Response()
	response.headers.add('Access-Control-Allow-Origin', '*')
	if result:
		response.status_code = 200
		return response
	else:
		response.status_code = 403
		return response

# Handle getting all tags
@app.route('/alltags', methods=['GET'])
def handle_get_alltags():
	response = jsonify(get_alltags(connection))
	response.headers.add('Access-Control-Allow-Origin', '*')
	return response