var zookeeper = require('node-zookeeper-client');
const mongo = require('mongoskin');
const moment = require('moment');
var thunkify = require('thunkify');
var co = require('co');

var router = require('koa-router')();
var koa = require('koa');

var app = koa();
app.use(router.routes());

const db = mongo.db("mongodb://101.200.130.59:27017/split", {
    native_parser: true
});

db.bind('streamInfo', {
    strict: true
});

db.bind('tasks', {
    strict: true
});

var id = process.argv[2];
console.log('id:' + id);

var client = zookeeper.createClient('127.0.0.1:2181');
var path = '/split/queue';
var znode = [];
var host = `123.123.123.${id}`;

const curTime = () => moment().format("YYYY-MM-DD HH:mm:ss");

//任务
const TASKS = {
  content: {},
  add: function(sid, pids) {
    //console.log(`${sid},${pids}`);
    //console.log(this);
    this.content[sid] = pids;
    this.save();
  },
  remove: function(sid) {
    if (this.content[sid] != null) {
      delete this.content[sid];
      this.save();
    }
  },
  save: function() {
    db.tasks.save({
      _id: host,
      tasks: this.content
    }, function(err, results) {
      if (err) {
        console.error(("[" + (curTime()) + "][Error]: while writing mongo db!\n") + err);
      }
    });
  }
};

var init = function() {

    db.tasks.findOne({
        _id: host
    }, function(err, data) {
        if (err) {
            console.log('err');
            console.log(err);
        } else {
            //console.log('data');
            //console.log(data);
            //tasks = data.tasks;
            var n = parseInt(id);
            //tasks = [0,1,2,3,4,5];//代表数据库
            var tasks = Array();
            for (var i = n*10+1; i < n * 10 + 11; i++) {
                console.log(i);
                tasks.push(i.toString());
            }
            co(function*() {
                for (index in tasks) {
                    task = tasks[index];
                    console.log(task);
                
                    let sid = task;
                    console.log(`${path}/${sid}`);
                    var exists = yield exists_sync(client, `${path}/${sid}`);
                    
                    console.log(exists);
                    if(exists){//节点存在
                        var data = yield getData_sync(client, `${path}/${sid}`, false);
                        //console.log(data);
                        if(data == host){//节点属于本机
                            var result = yield remove_sync(client, `${path}/${sid}`);
                            //var isWell = yield isAllWell(sid);//所有进程都正常
                            if(result){//节点删除成功，重新创建节点并监听
                                var result = yield createAndListen_sync(client, `${path}/${sid}`);
                                if(result){
                                   znode.push(sid);
                                   TASKS.content[sid] = sid;
                                }
                            }else{
                                console.log(`删除失败：${path}/${sid}`);
                            }
                            
                        }else{//节点不属于本机
                            console.log(`不在本机${host}：${path}/${sid}`);
                            console.log(`杀死进程：${sid}`);
                            //更新数据库Tasker
                        }
                    } else {//节点不存在
                        var result = yield createAndListen_sync(client, `${path}/${sid}`);
                        if(result){
                            znode.push(sid);
                            TASKS.content[sid] = sid;
                        }
                    }
                    //console.log(znode);
                    TASKS.save();
                    //console.log(znode);
                }
                console.log('init over');
            });
        }
    });
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
function start(sid) {
    co(function*() {
        var exists = yield exists_sync(client, `${path}/${sid}`);
        if (exists) { //节点存在
            var r = znode.indexOf(sid); //-1未找到
            if (r == -1) { //不在此主机
                var result = yield remove_sync(client, `${path}/${sid}`);
                if (result) { //节点删除成功，重新创建节点并监听
                    var result = yield createAndListen_sync(client, `${path}/${sid}`);
                    if (result) {
                        znode.push(sid);
                        TASKS.add(sid,sid);
                        //启动进程
                    }
                }
            } else {
                //启动进程
            }
        } else {
            var result = yield createAndListen_sync(client, `${path}/${sid}`);
            if (result) {
                znode.push(sid);
                TASKS.add(sid,sid);
            }
        }
    })
}

function stop(sid) {
    client.remove(`${path}/${sid}`, function(error) {
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

function restart(sid) {
    co(function*() {
        var exists = yield exists_sync(client, `${path}/${sid}`);
        if (exists) { //节点存在
            var r = znode.indexOf(sid); //-1未找到
            if (r == -1) { //不在此主机
                var result = yield remove_sync(client, `${path}/${sid}`);
                if (result) { //节点删除成功，重新创建节点并监听
                    var result = yield createAndListen_sync(client, `${path}/${sid}`);
                    if (result) {
                        znode.push(sid);
                        TASKS.add(sid,sid);
                        //重启进程
                    }
                }
            } else {
                //重启进程
            }
        } else {
            var result = yield createAndListen_sync(client, `${path}/${sid}`);
            if (result) {
                znode.push(sid);
                TASKS.add(sid,sid);
            }
        }
    })
}

function listChildren(client, path) {
    console.log('1');
    client.getChildren(
        path,
        function(event) {
            console.log('3');
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        function(error, children, stat) {
            console.log('4');
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
                for (var value of children.sort()) {
                    //getData(client, `${path}/${value}`);
                    console.log(yield getData_sync(client,`${path}/${value}`));

                }
            });
        }
    );
    console.log('2');
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

var QueueInit = (client, path) =>{
    //listChildren(client, `${path}/${host}`);
    client.getChildren(
        path,
        function(event) {
            listChildren(client, path);
        },
        function(error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log('Children of node: %s are: %j.', path, children);
            co(function *(){
                for (var value of children.sort()) {
                    //getData(client, `${path}/${value}`);
                    console.log(yield getData_sync(client,`${path}/${value}`));

                }
            });
        }
    );
}

var test = () =>{
    console.log(Date());
    console.log(`${path}/${host}`);
    co(function *(){
        var exists = yield exists_sync(client, `${path}/${host}`);
                        
        console.log(exists);
        if(exists){//节点存在
                    //     var data = yield getData_sync(client, `${path}/${sid}`, false);
                    //     //console.log(data);
                    //     if(data == host){//节点属于本机
                    //         var result = yield remove_sync(client, `${path}/${sid}`);
                    //         //var isWell = yield isAllWell(sid);//所有进程都正常
                    //         if(result){//节点删除成功，重新创建节点并监听
                    //             var result = yield createAndListen_sync(client, `${path}/${sid}`);
                    //             if(result){
                    //                znode.push(sid);
                    //                TASKS.content[sid] = sid;
                    //             }
                    //         }else{
                    //             console.log(`删除失败：${path}/${sid}`);
                    //         }
                            
                    //     }else{//节点不属于本机
                    //         console.log(`不在本机${host}：${path}/${sid}`);
                    //         console.log(`杀死进程：${sid}`);
                    //         //更新数据库Tasker
                    //     }
                    // } else {//节点不存在
                    //     var result = yield createAndListen_sync(client, `${path}/${sid}`);
                    //     if(result){
                    //         znode.push(sid);
                    //         TASKS.content[sid] = sid;
                    //     }
            QueueInit(client, `${path}/${host}`);//检测已有子节点，并执行任务
        }else{
            //var result = yield createAndListen_sync(client, `${path}/${host}`);
            var result = yield mkdirp_sync(client, `${path}/${host}`);
            if(result){//创建成功,新的节点下没有子节点

            }else{//创建失败
                //输出错误
            }
        }
    })

}

client.once('connected', function() {
    console.log('Connected to ZooKeeper.');
    //listChildren(client, path);
    //init();
    test();
    // client.mkdirp('/split/demo/1/2/3', function (error, path) {
    //     if (error) {
    //         console.log(error.stack);
    //         return;
    //     }
     
    //     console.log('Node: %s is created.', path);
    // });
});
// Array.prototype.remove = function(val) {
//     var index = this.indexOf(val);
//     if (index > -1) {
//         this.splice(index, 1);
//     }
// };
// 
router.get('/control', function *(next) {
        var query = this.request.query;
        console.log("[" + (curTime()) + "][info]: access " + this.request.url);
        var op = query.op;
        var sid = query.sid;
        var result;
        if (op !== 'start' && op !== 'stop' && op !== 'restart') {
            result = {
                ret: EINVALIDPARAM,
                msg: "Invalid params"
            };
            this.body = result;
            return;
        }
        if(op == 'start'){
            start(sid);
        }
        if(op == 'stop'){
            stop(sid);
        }
        if(op == 'restart'){
            restart(sid);
        }
        var result = {
            ret: 'OK',
            tasks: TASKS.content,
            znode: znode
        };
        this.body = result;
});

router.get('/', function *(next) {
        var result = {
            ret: 'OK',
            tasks: TASKS.content,
            znode: znode
        };
        this.body = result;
});

app.listen(`303${id}`);
console.log("[" + (curTime()) + "][Info]: Listening on 0.0.0.0:" + `303${id}`);
client.connect();
