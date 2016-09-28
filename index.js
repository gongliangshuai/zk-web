var zookeeper = require('node-zookeeper-client');
var router = require('koa-router')();
var koa = require('koa');
var thunkify = require('thunkify');
var co = require('co');

var app = koa();

var client = zookeeper.createClient('localhost:2181');
var path = '/tasker/'+process.argv[2];

client.once('connected', function () {
    console.log('Connected to the server.');
    listChildren(client, '/tasker');
 //    client.create(path, function (error) {
 //        if (error) {
 //            console.log('Failed to create node: %s due to: %s.', path, error);
 //        } else {
 //            console.log('Node: %s is successfully created.', path);
 //        }
	// });    
    //client.close();
});
//监听子节点
function listChildren(client, path) {
    client.getChildren(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        function (error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            //console.log(stat);
            console.log('Children of node: %s are: %j.', path, children);
        }
    );
}
//监听数据变化
function getData(client, path) {
    client.getData(
        path,
        function (event) {
            console.log('Got event: %s', event);
            getData(client, path);
        },
        function (error, data, stat) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
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

app.use(router.routes());

router.get('/create/:znode', function *(next) {
	console.log('Create');
	var znode = this.params.znode
    client.create(path+'/'+znode, function (rc, error) {
	    if (error) {
	        console.log('Failed to create node: %s due to: %s.', path, error);
	    } else {
	        console.log('Node: %s is successfully created.', path);
	    } 
	});

	var result = {'message':'Create'};
	this.body = result;
});

router.get('/delete/:znode', function *(next) {
	console.log('Delete');
	var znode = this.params.znode
	client.remove(path+'/'+znode, function (error) {
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
	var result = {'message':'Login'};
	this.body = result;
});

router.get('/exists', function *(next) {
	listChildren(client, path);
	var result = {'message':'Login'};
	this.body = result;
});

router.get('/getData', function *(next) {
	listChildren(client, path);
	var result = {'message':'Login'};
	this.body = result;
});

router.get('/setData/:data', function *(next) {
	var data = new Buffer(this.params.data);
	client.setData(path, data, function (error, stat) {
        if (error) {
            console.log('Got error when setting data: ' + error);
            return;
        }

        console.log(
            'Set data "%s" on node %s, version: %d.',
            data.toString(),
            path,
            stat.version
        );
    });
	var result = {'message':'SetData'};
	this.body = result;
});

router.get('/list', function *(next) {
	var path = '/tasker';
	client.getChildren(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
        },
        function (error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log('Children of node: %s are: %j.', path, children);
        }
    );
	var result = {'message':'Login'};
	this.body = result;
});

router.get('/sync', function *(next) {
	
	var result = {'message':'Login'};
	this.body = result;
});

router.get('/', function *(next) {
	var result = {'message':'Hello ZooKeeper'};
	this.body = result;
});

app.listen(process.argv[2]);
console.log('listen %s', process.argv[2]);

listChildren(client, path);
getData(client, path);
client.connect();