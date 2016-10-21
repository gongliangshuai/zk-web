var zookeeper = require('node-zookeeper-client');
const mongo = require('mongoskin');
const moment = require('moment');
var thunkify = require('thunkify');
var co = require('co');

var events=require('events');
var util=require('util');
var emitter = new events.EventEmitter();//创建了事件监听器的一个对象

var router = require('koa-router')();
var koa = require('koa');

var app = koa();
app.use(router.routes());

var id = process.argv[2];
console.log('id:' + id);

var client = zookeeper.createClient('127.0.0.1:2181');
var lockPath = '/split/lock';
var znode = [];
var host = `123.123.123.${id}`;
var owner;
var lock;

const curTime = () => moment().format("YYYY-MM-DD HH:mm:ss");

var init = function() {

}

function createAndListen(client,path,callback){
    client.create(path, new Buffer(host), zookeeper.CreateMode.EPHEMERAL, function (error) {
        if (error) {
            if(error.getCode() == zookeeper.Exception.NODE_EXISTS){
                console.log("Node exists.");
            }
            console.log(error.getCode());
            console.log('Failed to create node: %s due to: %s.', path, error);
            callback(null,'Failed to create node');
        } else {
            getData(client, path, true, callback);
            console.log('Node: %s is successfully created.', path);
        }

    });
}
const createAndListen_sync = thunkify(createAndListen);

function create(client,path,callback){
    client.create(path, new Buffer(host), 3, function (error, path) {
        if (error) {
            if(error.getCode() == zookeeper.Exception.NODE_EXISTS){
                console.log("Node exists.");
            }
            console.log(error.getCode());
            console.log('Failed to create node: %s due to: %s.', path, error);
            callback(null,false);
        } else {
            console.log('Node: %s is successfully created.', path);
            callback(null,path);
        }

    });
}
const create_sync = thunkify(create);

function remove(client,path,callback){
    client.remove(path, function (error) {
        if (error) {
            callback(null, false);
            return;
        }
        callback(null, true);
        console.log('Node: %s is deleted.', path);
    });
}
const remove_sync = thunkify(remove);

function mkdirp(client,path,callback){
    client.mkdirp(path, function (error, path) {
    if (error) {
        console.log(error.stack);
        callback(null, false);
        return;
    }
    callback(null, true);
    console.log('Mkdirp Node: %s is created.', path);
});
}
const mkdirp_sync = thunkify(mkdirp);

function getData(client, path, watch, callback) {
    client.getData(
        path,
        function(event) {
            if(watch){
                console.log('监听: %s', event);
                if (event.getType() == 2) {
                    console.log('节点已经删除: %s', path);
                    console.log('杀死进程: %s', path);
                    var arr = path.split('/');
                    //znode.remove(arr[arr.length-1]);
                    var index = znode.indexOf(arr[arr.length-1]);
                    if (index > -1) {
                        znode.splice(index, 1);
                        console.log(arr[arr.length-1]);
                        TASKS.remove(arr[arr.length-1]);
                    }
                    //console.log(znode);
                } else {
                    getData(client, path);
                }
            }else {
                console.log("不监听:%s", path);
            }
            
        },
        function(error, data, stat) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
                //getData(client, path);
                callback(error);
                return;
            }
            callback(null, data ? data.toString() : undefined);
            console.log(
                "Node: %s has data: %s, version: %d",
                path,
                data ? data.toString() : undefined,
                stat.version
            );
        }
    );
}
const getData_sync2 = thunkify(getData);

function exists(client, path, callback) {
    client.exists(
        path,
        function(error, stat) {
            if (error) {
                console.log(
                    'Failed to check existence of node: %s due to: %s.',
                    path,
                    error
                );
                callback(error,'failed');
                return;
            }

            if (stat) {
                callback(null,true);
                console.log(
                    'Node: %s exists.',
                    path
                );
                
            } else {
                callback(null,false);
                console.log('Node %s does not exist.', path);
            }
            
        }
    );
}
const exists_sync = thunkify(exists);

function listChildren(client, path) {
    client.getChildren(
        path,
        function(event) {
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        function(error, children, stat) {
            if (error) {
                if (error.code == '-101') {
                    
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
            co(function *(){
                console.log(children.sort()[0]);
                owner = `${lockPath}/${id}/`+children.sort()[0];
                emitter.emit(owner,owner);   //触发事件some_event
                if(owner == lock){
                    console.log('owner = lock');
                }
            });
        }
    );
}

function getData(client,path,callback){
    client.getData(
        path,
        function(event) {
            //getData(client, path);
        },
        function(error, data, stat) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
            }
            co(function *(){
                if(data){
                    try{
                        var cmd = JSON.parse(data);
                        console.log(cmd);
                        //执行命令
                        console.log('删除节点');
                        yield remove_sync(client, path);
                        callback(null,cmd);
                    } catch (e){
                        console.log("不是json！");
                        yield remove_sync(client, path);
                        callback(null,false);
                    }
                }else {
                    callback(null,false);
                    //console("没有数据！");
                    yield remove_sync(client, path);
                }
            })

        }
    );
}
const getData_sync = thunkify(getData);

function getLock(lock, callback) {
    let i = 0;
    // let timer = setInterval(function() {
    // i ++ ;
    // //if(i%100 == 0){
    //     console.log('等待:'+lock);
    //     console.log('当前:'+owner);
    // //}
    //     if(lock == owner){
    //         callback(false,true);
    //         clearInterval(timer);
    //     } 
    //     // if(owner == `${lockPath}/${id}/undefined`){
    //     //     clearInterval(timer);
    //     // }
    // }, 10);
    emitter.once(lock, function(value) {
        console.log('event lock:'+ value);
        setTimeout(function(){callback(false,true)},3000)
        //callback(false,true);
    });
    if(lock == owner){
        callback(false,true);
    } 
}
const getLock_sync = thunkify(getLock);

var LockInit = () =>{
    co(function *(){
    var exists = yield exists_sync(client, `${lockPath}/${id}`);
    if (!exists) { //节点不存在
      yield mkdirp_sync(client, `${lockPath}/${id}`);
    }
    listChildren(client, `${lockPath}/${id}`);
    });
}

client.once('connected', function() {
    console.log('Connected to ZooKeeper.');
    //listChildren(client, path);
    //init();
    //test();
    LockInit();
});

router.get('/control', function *(next) {
        var query = this.request.query;
        console.log("[" + (curTime()) + "][info]: access " + this.request.url);
        var op = query.op;
        var sid = query.sid;
        var result;
        var result = {
            ret: 'OK',
            znode: znode
        };
        this.body = result;
});

router.get('/', function*(next) {
    let cli = zookeeper.createClient('127.0.0.1:2181',{ sessionTimeout: 100 });
    cli.connect();
    //let emitter = new events.EventEmitter();//创建了事件监听器的一个对象
    // emitter.once('lock', function() {
    //     console.log('event: lock');
    // });
    let lock = yield create_sync(cli, `${lockPath}/${id}/lock-`);

    console.log('Created lock:'+lock);
    yield getLock_sync(lock);
    console.log('获得锁了！');
    yield remove_sync(cli, lock);
    console.log('删除锁了！');
    var result = {
        ret: 'OK',
        znode: znode
    };
    
    this.body = result;
    cli.close();
});

app.listen(`303${id}`);
console.log("[" + (curTime()) + "][Info]: Listening on 0.0.0.0:" + `303${id}`);
client.connect();


setTimeout(function(){
  //emitter.emit("lock");   //触发事件some_event
},3000);
