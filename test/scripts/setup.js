var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')
var path = require('path')
var Flume = require('flumedb')
var FlumeLog = require('flumelog-offset')
var FlumeViewLevel = require('flumeview-level')
var json = require('flumecodec/json')
var Index = require('../../')
var IndexTable = require('../../table')

var mkdirp = require('mkdirp')
var rmrf = require('rimraf')

var dir = '/tmp/test-normalized-index'
rmrf.sync(dir); mkdirp.sync(dir)

var N = 1000

var log = FlumeLog(path.join(dir, 'offset.log'), {codec: json})

var db = Flume(log).use('level', FlumeViewLevel(1, function (value) {
  return [value.hash]
}))

//var level = require('level')
//var db = level(path.join(dir, 'level'), {valueEncoding: 'json'})

function compare (a, b) {
  return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0
}

var crypto = require('crypto')

function hash (s) {
//  return s
  return crypto.createHash('sha256').update(s.toString()).digest('base64')
}

var index = Index(compare)

pull(
  log.stream({live: true}),
  pull.drain(function (data) {
    index.add({key:data.seq, value: data.value})
  })
)

var j = 0
;(function next (j) {
  if(j > N) return next2()
  console.log(j/N)
  var a = [], b = []
  for(var i = 0; i<N;i ++) {
    var value = {hash: hash(i+(j*N)), time:Date.now(), i: i, j:j}
    a.push(value)
//    b.push({key: value.hash, value: value, type: 'put'})
  }
//  db.batch(b, function (err) {
//    if(err) throw err
    log.append(a, function () {
      next(j + 1)
    })
//  })
})(0)

function next2 () {
  console.log('loaded')
  fs.writeFileSync(path.join(dir, 'table'), index.serialize())
}

