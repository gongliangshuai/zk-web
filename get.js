/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Copyrights licensed under the MIT License. See the accompanying LICENSE file
 * for terms.
 */

var zookeeper = require('node-zookeeper-client');

var client = zookeeper.createClient('127.0.0.1:2181', { retries : 2 });
var path = '/split/'+process.argv[2];

function getData(client, path) {
    client.getData(
        path,
        function (event) {
            console.log('Got event: %s', event);
            if(event.getType()==2){
                console.log('节点已经删除: %s', path);
            }else{
                getData(client, path);
            }
            
        },
        function (error, data, stat) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
                //getData(client, path);
                return;
            }

            console.log(
                'Node: %s has data: %s, version: %d',
                path,
                data ? data.toString() : undefined,
                stat.version
            );
        }
    );
}

client.once('connected', function () {
    console.log('Connected to ZooKeeper.');
    getData(client, path);
});

client.connect();
var sessionTimeout = client.getSessionTimeout();
console.log(sessionTimeout);
