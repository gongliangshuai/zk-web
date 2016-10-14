/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('127.0.0.1:2181', { retries : 2 });
var path = '/split/queue/localhost/qn-';//process.argv[2]

client.once('connected', function () {
    console.log(zookeeper.CreateMode);
    var data = '{"op":"start","sid":"1001"}';
    client.create(path, new Buffer(data), 2, function (error) {
        if (error) {
        	console.log(error.getCode());
        	if(error.getCode() == zookeeper.Exception.NODE_EXISTS){
        		console.log("Node exists.");
        	}
            console.log('Failed to create node: %s due to: %s.', path, error);
        } else {
            console.log('Node: %s is successfully created.', path);
        }

        client.close();
    });
});

client.connect();
