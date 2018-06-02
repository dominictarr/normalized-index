'use strict'
var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')

var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var path = require('path')
var rmrf = require('rimraf')
var mkdirp = require('mkdirp')
var Compact = require('../compact')

var dir = '/tmp/test-normalized-index_compact'
rmrf.sync(dir)
mkdirp.sync(dir)

var log = FlumeLog(path.join(dir, 'log'), {codec: json})

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var compactor = Compact(log, dir, compare)
var i = 0
pull(
  log.stream({live: true}),
  pull.drain(function (data) {
    compactor.add({key:data.seq, value: data.value})
  })
)

var data = [], N = 20000
for(var i = 0; i < N; i++)
  data.push({key: Math.random(), i: i, stage: 1, mod: !!(i%10)})

var data2 = []
for(var i = 0; i < N; i++)
  data2.push({key: Math.random(), i: i, stage: 2, mod: !!(i%10)})

var data3 = []
for(var i = 0; i < N; i++)
  data3.push({key: Math.random(), i: i, stage: 3, mod: !!(i%10)})

function equals(t, data) {
  pull(
    compactor.stream(),
    pull.collect(function (err, ary) {
      if(err) throw err
      t.deepEqual(ary, data.slice().sort(compare))
      t.end()
    })
  )
}

tape('initialize', function (t) {
  log.append(data, function () {
    equals(t, data)
  })
})

tape('compact', function (t) {
  var start = Date.now()
  compactor.compact(function (err, status) {
    console.log('compacted', Date.now() - start, status, compactor.status())
    equals(t, data)
  })
})

tape('write2', function (t) {
  log.append(data2, function () {
    equals(t, data.concat(data2))
  })
})

tape('compact2', function (t) {
  var start = Date.now()
  compactor.compact(function (err, status) {
    console.log('compacted2', Date.now() - start, status, compactor.status())
    equals(t, data.concat(data2))
  })
})

tape('write3', function (t) {
  log.append(data3, function () {
    equals(t, data.concat(data2).concat(data3))
  })
})

tape('compact3', function (t) {
  var start = Date.now()
  compactor.compact(function (err, status) {
    console.log('compacted3', Date.now() - start, status, compactor.status())
//    t.end()
    equals(t, data.concat(data2).concat(data3))
  })
})




