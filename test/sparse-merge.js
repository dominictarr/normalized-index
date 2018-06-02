'use strict'
var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')

var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var Index = require('../')
var Table = require('../table')
var FileTable = require('../file-table')
var Stream = require('../stream')
var SparseMerge = require('../sparse-merge')

var mkdirp = require('mkdirp')

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = FlumeLog(dir+'/log', {codec: json})

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var index1 = Index(compare)
var index2 = Index(compare)
var index3 = Index(compare)
var index4 = Index(compare)

var i = 0
var seqs = []
pull(
  log.stream({live: true}),
  pull.drain(function (data) {
    seqs.push(data)
    if((i++) % 10)
      index1.add({key:data.seq, value: data.value})
    else
      index2.add({key:data.seq, value: data.value})
  })
)

var data = [], N = 100
for(var i = 0; i < N; i++)
  data.push({key: Math.random(), i: i, mod: !!(i%10)})

log.append(data, function (err) {
  if(err) throw err

  function group (name, index1, index2) {
    name = name ? name +':' : ''
    tests(name+'mem1', index1, index2)
    tests(name+'mem2', index2, index1)
    var buffer1 = index1.serialize()
    var buffer2 = index2.serialize()
    var table1 = Table(buffer1, log, compare)
    var table2 = Table(buffer2, log, compare)
    tests(name+'table1', table1, table2)
    tests(name+'table2', index2, index1)
    tests(name+'table1,mem2', table1, index2)
    tests(name+'mem2,table1', index2, table1)
    tests(name+'table2,mem1', table2, index1)
    tests(name+'mem1,table2', index1, table2)

    var file = '/tmp/test-sparse-merge'+(name?'.'+name.replace(':','.'):'')
    try {
      fs.unlinkSync(file+1)
      fs.unlinkSync(file+2)
    } catch (err) {}
    fs.writeFileSync(file+1, buffer1)
    fs.writeFileSync(file+2, buffer2)

    var file1 = FileTable(file+1, log, compare)
    var file2 = FileTable(file+2, log, compare)

    tests('file1', file1, file2)
    tests('file2', file2, file1)
    tests('file1,mem2', file1, table2)
    tests('file1,table2', file1, table2)
    tests('file1,mem2', file1, index2)
    tests('mem2,file1', index2, file1)
    tests('file2,mem1', file2, index1)
    tests('mem1,file2', index1, file2)
  }
  group('', index1,index2)

  var sorted = seqs.slice().sort(function (a, b) {
    return compare(a.value, b.value)
  })
  //create two non-
  var a = sorted.slice(0, ~~(N/10))
  var b = sorted.slice(~~(N/10), sorted.length)
  a.forEach(function (data) {
    index3.add({key:data.seq, value: data.value})
  })
  b.forEach(function (data) {
    index4.add({key:data.seq, value: data.value})
  })

  a.forEach(function (e, i) {
    if(!~b.find(function (f) { return f.value.i == e.value.i })) throw new Error('same item:'+i)
  })

  group('non-overlapping', index3,index4)
})

function tests (name, index1, index2) {

  tape(name+':order', function (t)  {
    pull(
      Stream([index1, index2], {}, function (a, b) {
        return compare(a.value, b.value)
      }),
      pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, data.slice().sort(compare))
        t.end()
      })
    )
  })

  tape(name+':sparse-merge', function (t) {
    var c = 0, l = 0
    pull(
      SparseMerge(index1, index2),
      pull.through(function (e) {
        c++
      }),
      pull.map(function (e) {
        if(Array.isArray(e)) return e
        var ary = new Array(e.length/4)
        for(var i = 0; i < e.length/4; i++)
          ary[i] = e.readUInt32BE(i*4)
        return ary
      }),
      pull.flatten(),
      pull.asyncMap(log.get),
      pull.collect(function (err, ary) {
        if(err) throw err
        t.deepEqual(ary, data.slice().sort(compare))
        console.log(c, N/c) //number of ranges, and average range length.
        t.end()
      })
    )
  })
}





