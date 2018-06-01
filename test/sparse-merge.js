

var tape = require('tape')
var pull = require('pull-stream')

var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var Index = require('../')
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

tape('initialize', function (t) {
  log.append(data, t.end)
})

tape('order', function (t)  {
  pull(
    Stream([index1, index2], {}, compare),
    pull.collect(function (err, ary) {
      t.deepEqual(ary, data.slice().sort(compare))
      t.end()
    })
  )
})

tape('sparse-merge', function (t) {
  var c = 0, l = 0
  pull(
    SparseMerge(index1, index2),
    pull.through(function (e) {
      c++
    }),
    pull.flatten(),
    pull.asyncMap(log.get),
      pull.collect(function (err, ary) {
      t.deepEqual(ary, data.slice().sort(compare))
      console.log(c, N/c) //number of ranges, and average range length.
      t.end()
    })
  )
})

tape('non-overlapping', function (t) {
  var sorted = seqs.slice().sort(function (a, b) {
    return compare(a.value, b.value)
  })
  //create two non-
  sorted.slice(0, ~~(N/10)).forEach(function (data) {
    index3.add({key:data.seq, value: data.value})
  })
  sorted.slice(~~(N/10), sorted.length).forEach(function (data) {
    index4.add({key:data.seq, value: data.value})
  })

  var c = 0, l = 0
  pull(
    SparseMerge(index3, index4),
    pull.through(function (e) {
      console.log(e)
      c++
    }),
    pull.flatten(),
    pull.asyncMap(log.get),
      pull.collect(function (err, ary) {
      t.deepEqual(ary, data.slice().sort(compare))
      console.log(c, N/c) //number of ranges, and average range length.
      t.end()
    })
  )
})





