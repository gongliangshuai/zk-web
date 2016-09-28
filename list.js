/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('127.0.0.1:2181');
var path = '/split';

function listChildren(client, path) {
    console.log('1');
    var r = client.getChildren(
        path,
        function (event) {
            console.log('3');
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        function(error, children, stat) {
            console.log('4');
            if (error) {
                if (error.code == '-101') {
                    client.create(path, function(error) {
                        if (error) {
                            console.log('Failed to create node: %s due to: %s.', path, error);
                        } else {
                            console.log('Node: %s is successfully created.', path);
                        }

                        listChildren(client, path);
                    });
                } else {
                    console.log(
                        'Failed to list children of node: %s due to: %s.',
                        path,
                        error
                    );
                    console.log(error.code);
                }
                return;
            }

            console.log('Children of node: %s are: %j.', path, children);
        }
    );
    console.log('2');
    console.log('r:'+r);
}

client.once('connected', function () {
    console.log('Connected to ZooKeeper.');
    listChildren(client, path);
});

client.connect();
