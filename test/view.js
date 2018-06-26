var tape = require('tape')
var NormalizedIndex = require('../')
var pull = require('pull-stream')
var path = require('path')
var Flume = require('flumedb')
var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var mkdirp = require('mkdirp')
var rmrf = require('rimraf')

var crypto = require('crypto')

function hash (s) {
  return crypto.createHash('sha256').update(String(s)).digest('base64')
}

var dir = '/tmp/test-normalized-index_flumeview'

var string_codec = {
  decode: function (b) { return b.toString('ascii') }
}

rmrf.sync(dir)

var log = FlumeLog(path.join(dir, 'offset.log'), {codec: json})
var db = Flume(log)
  .use('index', NormalizedIndex(1, [['foo', 'bar']]))

var j = 0, N = 1000, M = 100000
;(function next (j) {
  if(j*N >= M) return next2()
  console.log(j/N)
  var a = [], b = []
  for(var i = 0; i<N;i ++) {
    var value = {
      hash: hash(i+(j*N)), time:Date.now(), i: i, j:j,
      foo: {bar: Math.random()}
    }
    a.push(value)
  }
  db.append(a, function () {
    next(j + 1)
  })
})(0)

function next2 () {
  console.log('next2')

  function random (target, limit, cb) {
    console.log('R', target)
    var start = Date.now(), c = 0, latency = 0
    pull(db.index.read({
      gte: {foo: {bar: target}},
      limit: 100
    }), pull.drain(function () {
      if(!latency) latency = Date.now() - start
      c++
    }, function () {
      var seconds = (Date.now() - start)/1000
      console.log('done', latency, seconds, c, c / seconds)
      cb()
    }))
  }
  setTimeout(function () {
    var i = 0
    random(Math.random(), 100, function again () {
      if(i++ > 100) return
      random(Math.random(), 100, again)
    })
  }, 1000)
}


