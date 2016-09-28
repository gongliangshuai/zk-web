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
    //listChildren(client, '/tasker');
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
function listChildren(client, path, callback) {
    client.getChildren(
        path,
        function (event) {
            console.log('Got Children watcher event: %s', event);
            if(event){
                listChildren(client, path);
            }
        },
        function(error, children, stat) { //监听增加节点
            if (error) {
                console.log(
                    'Failed to list children of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            console.log('Children of node: %s are: %j.', path, children);
            //console.log(children.length);
            var command = `znodes${path}`.replace(/\//g,'.znode_');
            console.log(command);
            //eval(`${command}={}`);
            //eval(`${command}.stat=stat`);
            eval(`${command}.children=children`);
            //eval(`${command}.data=data ? data.toString() : ''`);
            if (children && children.length) {
                //console.log('children:'+typeof children);
                //var childrens = children.toString().split(',');
                //console.log(children);
                for (var value of children) {
                    watchAll(client, `${path}/${value}`);
                    console.log(`监听节点:${path}/${value}`);

                }
            }
        }
    );
}
//监听数据变化
function getData(client, path, callback) {
    client.getData(
        path,
        function (event) {
            console.log('Got Data event: %s', event);
            getData(client, path);
        },
        function(error, data, stat) { //监听节点数据变化
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
            var command = `znodes${path}`.replace(/\//g,'.znode_');
            console.log(command);
            eval(`${command}={}`);
            //eval(`${command}.stat=stat`);
            //eval(`${command}.children[]=`);
            eval(`${command}.data=data ? data.toString() : ''`);
        }
    );
}
//监听是否存在
function exists(client, path, callback) {
    client.exists(
        path,
        function (event) {
            console.log('Got exists event: %s.', event);
            exists(client, path);
        },
        function(err,result){//监听删除节点
            if(err){
                console.log(`[exists]节点是否存在异常:${err}`);
            } else {
                console.log(`[exists]节点存在,数据:${result}`);
                getData(client, path, function(error, data, stat){});
                listChildren(client, path, function(error, children, stat){});
            }

        }
    );
}

//监听所有节点
function watchAll(client, path){
    // exists(client, path, function(err, result) {});
    // if (err) {
    //     console.log(`[exists]节点是否存在异常:${err}`);
    // } else {
    //     console.log(`[exists]节点存在,数据:${result}`);
    //     getData(client, path, function(error, data, stat) {});
    //     listChildren(client, path, function(error, children, stat) {});
    // }
    getData(client, path, function(error, data, stat) {});
    listChildren(client, path, function(error, children, stat) {});
}

//拆分字符串方法
function decode(str){
    var arr = str.split('@');
    var op = arr[0];
    var path = arr[1];
    return ;
}

var znodes = {
    children:{'sid':{}},
    stat:{},
    data:{}
}

setInterval(function(){console.log(znodes)},1000); 

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

//app.listen(process.argv[2]);
//console.log('listen %s', process.argv[2]);

watchAll(client, '/tasker');
//getData(client, path);
client.connect();