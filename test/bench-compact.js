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
var SparseMerge = require('../sparse-merge')
var Stream = require('../stream')

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

var random = new Buffer(1024).toString('base64')

var very_start = Date.now()

;(function next (j) {
  var data = [], N = 10000
  if(j*N>1000000) return console.log('done!') 
  for(var i = 0; i < N; i++)
    data.push({key: Math.random(), i: i, stage: 1, mod: !!(i%10), text: random})
  var start = Date.now()
  log.append(data, function () {
    console.log('write', Date.now() - start)
    start = Date.now()
  /*  if(compactor.indexes.length > 1)
      pull(
        SparseMerge(compactor.indexes[0], compactor.indexes[1]),
//        Stream(compactor.indexes[0]),
        pull.drain(null, function () {
          console.log('--merge', Date.now() - start)
          next2()
        })
      )
    else*/
      next2()

    function next2 () {
      compactor.compact(function (err, status) {
        if(err) throw err
        console.log('compact', Date.now() - start, status)
        var seconds = (Date.now() - very_start)/1000
        var mb = (status.since/(1024*1024))
        console.log('mb/second', mb / seconds, mb, seconds)
        console.log(process.memoryUsage().heapUsed/(1024*1024))
        next(j+1)
      })
    }
  })
})(0)


