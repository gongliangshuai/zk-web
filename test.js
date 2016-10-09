var fetch = require('node-fetch');
var url = 'http://127.0.0.1:3000/control?op=start&sid=101';
var n = 1;
var tasks = {
  '1': Array(),
  '2': Array(),
  '3': Array()
};
console.log(tasks);
for (index in tasks) {
  console.log(index);
  for (var i = parseInt(index) * 10 + 1; i <= parseInt(index) * 10 + 10; i++) {
    //console.log(i);
    tasks[index].push(i.toString());
  }
}

console.log(tasks);

function random(min, max) {
  return Math.floor(Math.random() * (max - min) + min)
}

function randomOp() {
  let n = Math.floor(Math.random() * 3);
  switch (n) {
    case 0:
      return 'start';
      break;
    case 1:
      return 'start';
      return 'restart';
      //return 'stop';
      break;
    case 2:
      return 'restart';
      break;
  }
}

function remove(sid) {
  for (index in tasks) {
    //console.log(index);
    let i = tasks[index].indexOf(sid);
    if (i > -1) {
      tasks[index].splice(i, 1);
    }
    tasks[index].sort();
  }
}

setInterval(function() {
  let i = n++ % 3 + 1;
  let op = randomOp();
  let sid = random(11, 40).toString();
  console.log(`${i}-${op}-${sid}`);
  switch (op) {
    case 'start':
      remove(sid);
      tasks[i.toString()].push(sid);
      tasks[i.toString()].sort();
      break;
    case 'stop':
      remove(sid);
      break;
    case 'restart':
      remove(sid);
      tasks[i.toString()].push(sid);
      tasks[i.toString()].sort();
      break;
    default :
      console.log(op);
  }

  fetch(`http://127.0.0.1:303${i}/control?op=${op}&sid=${sid}`)
    .then(function(res) {
      return res.json();
    }).then(function(json) {
      //console.log(json);
      console.log(tasks);
    });
}, 300);