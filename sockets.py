#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2013-2014 Abram Hindle
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import flask
from flask import Flask, request, make_response
from flask_sockets import Sockets
import gevent
from gevent import queue
import time
import json
import os

app = Flask(__name__)
sockets = Sockets(app)
app.debug = True

class Client:
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, v):
        self.queue.put_nowait(v)

    def get(self):
        return self.queue.get()

class World:
    def __init__(self):
        self.clear()
        # we've got listeners now!
        self.listeners = list()
        
    def add_set_listener(self, listener):
        self.listeners.append( listener )

    def update(self, entity, key, value):
        entry = self.space.get(entity,dict())
        entry[key] = value
        self.space[entity] = entry
        self.update_listeners( entity )

    def set(self, entity, data):
        self.space[entity] = data
        self.update_listeners( entity )

    def update_listeners(self, entity):
        '''update the set listeners'''
        for listener in self.listeners:
            listener(entity, self.get(entity))

    def clear(self):
        self.space = dict()

    def get(self, entity):
        return self.space.get(entity,dict())
    
    def world(self):
        return self.space

myWorld = World()        

def set_listener( entity, data ):
    ''' do something with the update ! '''
    #for client in clients:
    #	client.put(json.dumps(myWorld.world()))
    message = json.dumps({entity: data})
    for client in clients:
        client.put(message)

clients = list()
myWorld.add_set_listener( set_listener )      

@app.route('/')
def hello():
    '''Return something coherent here.. perhaps redirect to /static/index.html '''
    return flask.redirect("/static/index.html")

# read_ws function from github.com/abramhindle/WebSocketExamples/blob/master/chat.py
def read_ws(ws,client):
    '''A greenlet function that reads from the websocket and updates the world'''
    # OOO: TODO IMPLEMENT ME
    try:
    	while True:
    		msg = ws.receive()
    		print "RECV: %s" % msg
    		if(msg != None):
    			packet = json.loads(msg)
    			# send_all_json( packet ) use a send_all_json equiv for this assn
    			# iter through and set world stuff to entity according to key
    			for name, data in packet.iteritems():
    				entity = myWorld.get(name)

    				for k, v in data.iteritems():
    					entity[k] = v
    				myWorld.set(name,entity)
    		else:
    			print "MSG: %s" %msg
    			break
    except:
    	''' Done '''
    return None

# subscribe function https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
@sockets.route('/subscribe')
def subscribe_socket(ws):
    '''Fufill the websocket URL of /subscribe, every update notify the
       websocket and read updates from the websocket '''
    # OOO: TODO IMPLEMENT ME
    client = Client() #
    clients.append(client)
    client.put(json.dumps(myWorld.world())) #put world to client
    event = gevent.spawn(read_ws, ws, client)
    try:
        while True:
            msg = client.get()
            print "Message: %s" %msg
            ws.send(msg)
	    		
    except Exception as e: #ws error as e
        print "WS Error %s" %e
    finally:
        clients.remove(client)
        gevent.kill(event)
    #return None


def flask_post_json():
    '''Ah the joys of frameworks! They do so much work for you
       that they get in the way of sane operation!'''
    if (request.json != None):
        return request.json
    elif (request.data != None and request.data != ''):
        return json.loads(request.data)
    else:
        return json.loads(request.form.keys()[0])

@app.route("/entity/<entity>", methods=['POST','PUT'])
def update(entity):
    '''update the entities via this interface'''
    jsonData = flask_post_json(request)

    for key, value in jsonData.iteritems():
    	myWorld.update(entity, key, value)
    return create_response(myWorld.get(entity))

# responser from ajax assignment
def create_response(data):
	response = make_response(json.dumps(data), 200)
	response.headers['Content-Type'] = 'application/json'
	return response  

@app.route("/world", methods=['POST','GET'])    
def world():
    '''you should probably return the world here'''
    return create_response(myWorld.world())

@app.route("/entity/<entity>")    
def get_entity(entity):
    '''This is the GET version of the entity interface, return a representation of the entity'''
    return create_response(myWorld.get(entity))


@app.route("/clear", methods=['POST','GET'])
def clear():
    '''Clear the world out!'''
    myWorld.clear()
    return create_response(myWorld.world())



if __name__ == "__main__":
    ''' This doesn't work well anymore:
        pip install gunicorn
        and run
        gunicorn -k flask_sockets.worker sockets:app
    '''
    app.run()
