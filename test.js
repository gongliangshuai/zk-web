var fetch = require('node-fetch');
var url = 'http://127.0.0.1:3000/control?op=start&sid=101';

function random(min,max){
  return Math.floor(Math.random() * (max - min) + min)
}

function randomOp(){
  let n = Math.floor(Math.random() * 3);
  switch (n) {
    case 0:
      return 'start';
      break;
    case 1:
      return 'stop';
      break;
    case 2:
      return 'restart';
      break;
  }
}

setInterval(function() {
  fetch(`http://127.0.0.1:3031/control?op=${randomOp()}&sid=${random(101,400)}`)
    .then(function(res) {
      return res.json();
    }).then(function(json) {
      console.log(json);
    });
}, 1000);

setInterval(function() {
  fetch(`http://127.0.0.1:3032/control?op=${randomOp()}&sid=${random(101,400)}`)
    .then(function(res) {
      return res.json();
    }).then(function(json) {
      console.log(json);
    });
}, 1000);

setInterval(function() {
  fetch(`http://127.0.0.1:3033/control?op=${randomOp()}&sid=${random(101,400)}`)
    .then(function(res) {
      return res.json();
    }).then(function(json) {
      console.log(json);
    });
}, 1000);