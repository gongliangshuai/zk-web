var zookeeper = require('node-zookeeper-client');
const mongo = require('mongoskin');
var thunkify = require('thunkify');
var co = require('co');

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
var znode = [];
var host = '123.123.123.1'

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
            tasks = [0,1,2,3,4,5];//代表数据库
            for (task in tasks) {
                console.log(task);
                co(function*() {
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
                        }
                    }
                    console.log(znode);
                });
                console.log(task);

            }
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
        }
        callback(null, true);
        console.log('Node: %s is deleted.', path);
    });
}
const remove_sync = thunkify(remove);

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
                    }
                    console.log(znode);
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
const getData_sync = thunkify(getData);

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
            }
        }
    })
}

function listChildren(client, path) {
    console.log('1');
    var r = client.getChildren(
        path,
        function(event) {
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
    console.log('r:' + r);
}

function listen(client,path){
    client.getData(
        path,
        function(event) {
            getData(client, path);
        },
        function(error, data, stat) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
            }
            if(data){
                try{
                    var cmd = JSON.parse(data);
                    console.log(cmd);
                } catch (e){
                    console.log("不是json！");
                }
            }else {
                //console("没有数据！");
            }

        }
    );
}

var test = () =>{
    console.log(Date());
    var n = "n";
    co(function *(){
        console.log("1:"+n);
        try{
            var exists = yield exists_sync(client, `${path}/0`);
        } catch (e){
            console.log(e);
        }
        
        console.log(exists);
        console.log("2:"+Date());
    });
    console.log("3:"+Date());
}

client.once('connected', function() {
    console.log('Connected to ZooKeeper.');
    //listChildren(client, path);
    init();
    //test();
});
// Array.prototype.remove = function(val) {
//     var index = this.indexOf(val);
//     if (index > -1) {
//         this.splice(index, 1);
//     }
// };
client.connect();