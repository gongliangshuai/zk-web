/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('127.0.0.1:2181', { retries : 2 });
var path = '/split/'+process.argv[2];

client.on('connected', function (state) {
    console.log('Connected to the server.');
    client.remove(path, function (error) {
        if (error) {
            console.log(
                'Failed to delete node: %s due to: %s.',
                path,
                error
            );
            return;
        }

        console.log('Node: %s is deleted.', path);
        client.close();
    });
});

client.connect();
