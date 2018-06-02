
'use strict'
var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')

var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var path = require('path')
var rmrf = require('rimraf')
var mkdirp = require('mkdirp')
var Index = require('../index')
var FileTable = require('../file-table')
//var Compact = require('../compact')
var SparseMerge = require('../sparse-merge')
var WriteFile = require('pull-write-file')
var Stream = require('../stream')
var Group = require('pull-group')
var dir = '/tmp/test-normalized-index_levels'
var Cache = require('hashlru')
rmrf.sync(dir)
mkdirp.sync(dir)

var log = FlumeLog(path.join(dir, 'log'), {codec: json})

var _get = log.get, gets = 0
//var cache = Cache(1024)
//log.get = function (offset, cb) {
//  gets++
//  var value = cache.get(offset)
//  if(value) return cb(null, value)
//  _get(offset, function (err, value) {
//    if(err) return cb(err)
//    cache.set(offset, value)
//    cb(err, value)
//  })
//}

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0 || a.key2 - b.key2
}
//var compactor = Compact(log, dir, compare)
var i = 0
var index = Index(compare)
pull(
  log.stream({live: true}),
  pull.drain(function (data) {
    index.add({key:data.seq, value: data.value})
  })
)

var random = new Buffer(10).toString('base64')

var very_start = Date.now()

function shuffel (data) {
  return data.sort(function () { return Math.random() - 0.5 })
}
var J = 5, I = 0
;(function next (j) {
  var data = [], N = 10000
  if(j>J) return next2()
  for(var i = 0; i < N*Math.pow(j, 1.5); i++) {
    var d = data[~~(Math.pow(Math.random(), 4)*data.length)]
//    console.log(d && d.random)
    data.push({
      key: ~~(Math.pow(Math.random(), 3)*1000),//,d ? d.random : 0,
        //I++ + (Math.random() - 0.5) * 10000,
      key2: I+++ (Math.random() - 0.5) * 1000,
      random: Math.random(),
      i: i, stage: j, mod: !!(i%10),
      text: random
    })
//    console.log(i/N*Math.pow(j, 1.7))
  }
  var start = Date.now()
  log.append(shuffel(data), function () {
    console.log('write', 'level'+j, Date.now()-start)
    var b = index.serialize()
    fs.writeFile(path.join(dir, 'level'+j), b, function (err, written) {
      if(err) throw err
      console.log('written', b.length)
      index = Index(compare)
      next(j+1)

    })
  })
})(1)

var indexes = []
function next2 () {
  console.log('NEXT2')
  for(var j = 1; j <= J; j++) {
    console.log('open', 'level'+j)
    indexes.push(FileTable(path.join(dir, 'level'+j), log, compare))
  }
  setTimeout(function () {
    var start = Date.now()
    gets = 0
    pull(
      Stream(indexes, {gt: {key: 0}}, function (a, b) { return compare(a.value, b.value) }),
//      pull.take(20),
      pull.drain(null, function () {
        console.log('dump', Date.now()-start, gets)
        gets = 0
        next_merge()
      })
    )
  }, 1000)
}

function next_merge (i) {
  i = i || 1
  console.log('indexes:',indexes.length)
  var a = indexes.shift()
  var b = indexes.shift()
  console.log('merge', i)
  if(!b) return console.log('done')
  var filename = path.join(dir, 'level_merge'+i)
  var start = Date.now(), c = 0, t = 0,K=0
  pull(
    SparseMerge(a, b, compare),
    pull.map(function (ary) {
      c ++; t += Buffer.isBuffer(ary) ? ary.length/4 : ary.length 
      if(Buffer.isBuffer(ary)) return ary
      var buf = new Buffer(ary.length*4)
      for(var i = 0; i < ary.length; i++)
        buf.writeUInt32BE(ary[i], i*4)
      return buf
    }),
    //I have two modes here, because I was trying to understand
    //why sparse-merge was slow. answer: it's not good with uniform randomness.
    false
    ?  pull.collect(function (err, ary) {
        console.log('merged', c, t, t/c, Date.now()-start, gets)
        var b =Buffer.concat(ary)
        start = Date.now()
        fs.writeFile(filename, b, function (err) {
          console.log('written:', b.length, Date.now()-start)
          var new_table = FileTable(filename, log, compare)
          indexes.unshift(new_table)
          new_table.ready(function () {
            next_merge(i+1)
          })
        })
      })
    : pull(
//        Group(100),
//        pull.map(function (ary) {
//          K++
//          return Buffer.concat(ary)
//        }),
        WriteFile(filename, function (err, val) {
          if(err) throw err
          console.log('merged', c, t, t/c, Date.now()-start, gets)
            var new_table = FileTable(filename, log, compare)
          indexes.unshift(new_table)
          new_table.ready(function () {
            next_merge(i+1)
          })
        })
      )
  )
//    Group(1024),
/*
    
  )
*/
}



