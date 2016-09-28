/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('127.0.0.1:2181', { retries : 2 });
var path = '/split/'+process.argv[2];

client.once('connected', function () {
    console.log('Connected to the server.');

    client.create(path, new Buffer('data'), zookeeper.CreateMode.EPHEMERAL, function (error) {
        if (error) {
            console.log('Failed to create node: %s due to: %s.', path, error);
        } else {
            console.log('Node: %s is successfully created.', path);
        }

        //client.close();
    });
});

client.connect();
