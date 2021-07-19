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
#
# Flask web app for archipel - decentralized tagging of dis/interested spaces.
#
###

###
# Dependencies
###
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.exc import IntegrityError
from flask import Flask, request, Response, jsonify

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

# Add tag (requires tag_name and location)
def add_tag(connection, tag_name, lat, lon):
    try:
        q = text('''INSERT INTO tags(tag_name, lat, lon)
            VALUES(:tag_name, :lat, :lon)''')
        connection.execute(q, tag_name=tag_name, lat=lat, lon=lon)
    except IntegrityError:
        return False
    return True

# Add message to tag (requires tag_name and message text)
def add_msg(connection, tag_name, msg):
    try:
        q = text('''INSERT INTO messages(tag_name, msg)
            VALUES(:tag_name, :msg)''')
        connection.execute(q, tag_name=tag_name, msg=msg)
    except IntegrityError:
        return False
    return True

# Get all messages (with their timestamps), location and timestamp from a tag
def get_tag(connection, tag_name):
    q = text('''SELECT lat, lon, msg, t.created_at tag_created_at, m.created_at msg_created_at
        FROM tags t LEFT JOIN messages m ON m.tag_name = t.tag_name
        WHERE t.tag_name = :tag_name''')
    result = connection.execute(q, tag_name=tag_name).fetchall()
    if(len(result) < 1) :
        return None
    lat, lon = result[0][0], result[0][1]
    created_at = str(result[0][3])
    messages = [{'text': str(row[2]), 'created_at': str(row[4])} for row in result
        if row[2] is not None]
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

# Util to allow sensible CORS for a response
def allow_cors(response):
    response.headers.add("Vary", "Origin")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type")
    if request.origin == None:
        return
    if "localhost" in request.origin:
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8000')
    else:
        response.headers.add('Access-Control-Allow-Origin', 'https://deveringham.github.io')


###
# Main routine
###

# Create sqlalchemy engine
engine = create_engine('mysql://' + DB_USER +
    ':' + DB_PASS +
    '@localhost:' + str(DB_PORT) +
    '/' + DB_NAME)
connection = engine.connect()

# Select the archipel db
connection.execute(text('USE archipel'))

# Delete tables
#connection.execute(text('DROP TABLE IF EXISTS messages'))
#connection.execute(text('DROP TABLE IF EXISTS tags'))

# Initialize timezone for timestamp columns
connection.execute(text('SET time_zone = \'+00:00\''))

# Create tables, if they don't already exist
connection.execute(text('''CREATE TABLE IF NOT EXISTS tags
    (tag_name VARCHAR(25) PRIMARY KEY,
    lat DOUBLE NOT NULL, lon DOUBLE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'''))
connection.execute(text('''CREATE TABLE IF NOT EXISTS messages
    (msg_id INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(25),
    msg VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_name) REFERENCES tags(tag_name) ON DELETE CASCADE)'''))

# Clear the tables at startup
#clear_all(connection)

# Add some test junk to DB
# add_tag(connection, 'test', 52.520008, 13.404954)
# add_msg(connection, 'test', 'test_msg_0')
#add_msg(connection, 'test', 'test_msg_1')
#add_tag(connection, 'test2', 52.52, 13.41)

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
    if request.method == 'GET':
        result = get_tag(connection, tag_name)
        if result == None:
            response.status_code = 404
        else:
            response = jsonify(result)
    else:
        result = add_tag(connection, tag_name)
        if result:
            response.status_code = 201
        else:
            response.status_code = 403
    allow_cors(response)
    return response

# Handle adding a new message to a tag (POST)
@app.route('/msg/<tag_name>', methods=['POST', 'OPTIONS'])
def handle_add_msg(tag_name):
    if request.method == 'POST':
        msg = request.json['text']
        result = add_msg(connection, tag_name, msg)
        if result:
            tag = get_tag(connection, tag_name)
            response = jsonify(tag)
            allow_cors(response)
            return response
        else:
            response.status_code = 403
            return response
    elif request.method == 'OPTIONS':
        response = Response()
        allow_cors(response)
        response.content_type = "application/json"
        return response

# Handle getting all tags (GET)
@app.route('/alltags', methods=['GET'])
def handle_get_alltags():
    response = jsonify(get_alltags(connection))
    allow_cors(response)
    return response
