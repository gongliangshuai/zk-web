/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('localhost:2181', { retries : 2 });
var path = '/tasker';
var acls = [
    new zookeeper.ACL(
        zookeeper.Permission.ADMIN,
        new zookeeper.Id('world', 'anyone')
    )
];

client.on('connected', function (state) {
    console.log('Connected to the server.');

    client.setACL(path, acls, -1, function (error, stat) {
        if (error) {
            console.log('Failed to set ACL: %s.', error);
            return;
        }

        console.log('ACL is set to: %j', acls);

        client.getACL(path, function (error, acls, stat) {
            if (error) {
                console.log('Failed to get ACL: %s.', error);
                return;
            }

            console.log('ACL of node: %s is: %j', path, acls);
            client.close();
        });
    });
});

client.connect();

