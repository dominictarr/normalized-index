'use strict'
var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')
var leftpad = require('left-pad')
var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var path = require('path')
var rmrf = require('rimraf')
var mkdirp = require('mkdirp')
var Compact = require('../compact')
var SparseMerge = require('../sparse-merge')

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

/*
*****
create a database with M random records, writing in batches of N,
and then create an index for the key field (which is uniformly random)

after every batch, run the compactor.
*/

var N = 10000, M = 500000

;(function next (j) {
  var range_start = j*N, range_end = (j+1)*N
  var range_string = '('+range_start+','+range_end+')'
  var data = []
  if(j*N>M) return console.error('total_time()', Date.now() - very_start) 
  for(var i = 0; i < N; i++)
    data.push({
      //key: 'K'+i,
//      key: 'K'+leftpad(i, 10, '0'), //Date.now(),
//      key: 'J'+(~(Math.random()*10))+ '='+leftpad(i*j, 10, '0'),
      //key: 'J'+(~(Math.random()*10)),//+ '='+leftpad(i*j, 10, '0'),
//      key: 'J'+(~(Math.random()*10))+"_"+Date.now() + '='+i,
//      key: (~(Math.random()*100))+"_"+leftpad(i, 8), //Date.now(),
      key: (~(Math.random()*10000000))+"_"+Date.now(),
      i: i, j: j, stage: 1, mod: !!(i%10),
      text: random
    })
  var start = Date.now()
  log.append(data, function () {
    //time to perform the last
    console.log('log-write-time'+range_string, Date.now() - start)
    start = Date.now()
    compactor.compact(function (err, status) {
      if(err) throw err
      console.log('compaction-time'+range_string, Date.now() - start, status)
      var seconds = (Date.now() - very_start)/1000
      var mb = (status.since/(1024*1024))
      console.log('mb/second', mb / seconds, mb, seconds)
      console.log('memory', process.memoryUsage().heapUsed/(1024*1024))
      next(j+1)
    })
  })
})(0)


