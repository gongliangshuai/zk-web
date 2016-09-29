var zookeeper = require('node-zookeeper-client');
const mongo = require('mongoskin');

const db = mongo.db("mongodb://101.200.130.59:27017/split", {
  native_parser: true
});

db.bind('streamInfo', {
  strict: true
});

db.bind('tasks', {
  strict: true
});

var client = zookeeper.createClient('127.0.0.1:2181');
var path = '/split';
var znode = {};

db.tasks.findOne({
    _id: '123.56.68.146'
}, function(err, data) {
    if (err) {
        console.log('err');
        console.log(err);
    } else {
        //console.log('data');
        //console.log(data);
        tasks = data.tasks;
        for (sid in tasks) {
            console.log(sid);
            client.create(`${path}/${sid}`, new Buffer('123.56.68.146'), zookeeper.CreateMode.EPHEMERAL, function (error, path) {
                if (error) {
                    if(error.getCode() == zookeeper.Exception.NODE_EXISTS){
                        console.log("Node exists.");
                    } else {
                       console.log('Failed to create node: %s due to: %s.', path, error);
                    }
                    
                } else {
                    getData(client, path);
                    console.log('Node: %s is successfully created.', path);
                }

                //client.close();
            });
        }
    }
});


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
                "Node: %s has data: %s, version: %d",
                path,
                data ? data.toString() : undefined,
                stat.version
            );
        }
    );
}

function exists(client, path) {
    client.exists(
        path,
        function (event) {
            console.log('Got event: %s.', event);
            exists(client, path);
        },
        function (error, stat) {
            if (error) {
                console.log(
                    'Failed to check existence of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            if (stat) {
                console.log(
                    'Node: %s exists and its version is: %j',
                    path,
                    stat.version
                );
            } else {
                console.log('Node %s does not exist.', path);
            }
        }
    );
}

function start(sid) {
    client.exists(
        path,
        function (event) {
            console.log('Got event: %s.', event);
            exists(client, path);
        },
        function (error, stat) {
            if (error) {
                console.log(
                    'Failed to check existence of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            if (stat) {
                console.log(
                    'Node: %s exists and its version is: %j',
                    path,
                    stat.version
                );
            } else {
                console.log('Node %s does not exist.', path);
            }
        }
    );
}

function stop(sid){
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
    });
}

function restart(sid){

}

function listChildren(client, path) {
    console.log('1');
    var r = client.getChildren(
        path,
        function (event) {
            console.log('3');
            console.log('Got watcher event: %s', event);
            //listChildren(client, path);
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
            for (var value of children) {
                getData(client, `${path}/${value}`);
            }
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
