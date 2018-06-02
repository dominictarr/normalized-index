var FlumeLog = require('flumelog-offset')
var pull = require('pull-stream')
var tape = require('tape')
var mkdirp = require('mkdirp')
var Compact = require('../compact')
var rimraf = require('rimraf')

var dir = '/tmp/test_compact'
rimraf.sync(dir)
var index_dir = '/tmp/test_compact/index'
var index_dir2 = '/tmp/test_compact/index2'
mkdirp.sync(dir)
mkdirp.sync(index_dir)
mkdirp.sync(index_dir2)

var log = FlumeLog(dir+'/log', {codec: require('flumecodec/json')})

var n = 4
var a = [
  {foo: true, bar: false, r: Math.random()},
  {foo: true, bar: true, r: Math.random()},
  {foo: false, bar: true, r: Math.random()},
  {foo: false, bar: false, r: Math.random()}
]

log.append(a[0], next)
log.append(a[1], next)
log.append(a[2], next)
log.append(a[3], next)

function cmp (a, b) {
  return a < b ? -1 : a > b ? 1 : 0
}

function compare (a, b) {
  return cmp(a.foo, b.foo) || cmp(a.bar, b.bar)
}

function compare2 (a,b) {
  return cmp(a.r, b.r) || compare(a, b)
}

//var c = Compact(log, index_dir, compare, decode)
var c2 = Compact(log, index_dir2, compare2)

pull(
  log.stream({live: true, keys: true, values: true}),
  pull.drain(function (data) {
    c2.add({key:data.seq, value: data.value})
  })
)

function next () {
  if(--n) return
  tape('compact', function (t) {
    c2.compact(function (err) {
      if(err) throw err
      setTimeout(function () {
        pull(c2.stream({keys: true, live: false}), pull.collect(function (err, ary) {
          if(err) throw err
          console.log(ary)
          console.log(a)
          t.deepEqual(ary, a)
          t.end()
        }))
      })
    })

    c2.compact(function (err) {
      console.log('done')
    })
  })
}

