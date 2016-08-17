var tape = require('tape')

var Offset = require('offset-log')
var Index = require('../')
var IndexTable = require('../table')
var Stream = require('../stream')
var pull = require('pull-stream')
var mkdirp = require('mkdirp')

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = Offset(dir+'/log')
console.log(dir)

function encode (value) {
  return new Buffer(JSON.stringify(value))
}

function decode (value) {
  return JSON.parse(value.toString())
}

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var index = Index(log, compare, decode)

var alpha = 'abcdefghijklmnopqrstuvwxyz'
var a = []
for(var i = 0; i + 2 < alpha.length; i++)
  a.push({key: alpha[i] + alpha[i+1] + alpha[i+2], seq: i+10})

tape('alphabet', function (t) {

  //sort randomly
  a.sort(function () {
    return Math.random() - 0.5
  })

  var i = 0
  ;(function loop () {
    console.log(a[i])
    if(i === a.length) t.end()
    else log.append(encode(a[i++]), loop)
  })()

})

tape('everything', function (t) {
  pull(
    Stream(index, {}),
    pull.collect(function (err, ary) {
      if(err) throw err
      console.log(ary)
      t.equal(ary.length, 24)
      t.deepEqual(ary, ary.slice().sort(compare))
      t.end()
    })
  )
})
tape('everything, reversed', function (t) {
  pull(
    Stream(index, {reverse: true}),
    pull.collect(function (err, ary) {
      if(err) throw err
      console.log(ary)
      t.equal(ary.length, 24)
      t.deepEqual(ary, ary.slice().sort(compare).reverse())
      t.end()
    })
  )
})

tape('stream to half-way', function (t) {
  var i = ~~(Math.random()*a.length)
  var target = a[i]
  pull(
    Stream(index, {gte: target}),
    pull.collect(function (err, ary) {
      if(err) throw err
      ary.forEach(function (e) {
        t.ok(compare(e, target) >= 0, 
          e.key + ' >= ' +target.key
        )
      })
      t.end()
    })
  )
})

tape('stream middle range', function (t) {
  var i = ~~(Math.random()*a.length)
  var j = ~~(Math.random()*a.length)
  var start = a[Math.min(i, j)]
  var end = a[Math.max(i, j)]
  pull(
    Stream(index, {gt: start, lt: end}),
    pull.collect(function (err, ary) {
      if(err) throw err
      ary.forEach(function (e) {
        t.ok(compare(e, start) > 0, e.key+'>'+start.key)
        t.ok(compare(e, end) < 0, e.key+'<'+end.key)
      })
      t.end()
    })
  )
})

tape('stream to half-way, reverse', function (t) {
  var i = ~~(Math.random()*a.length)
  var target = a[i]
  pull(
    Stream(index, {gte: target, keys: true, values: false}),
    pull.collect(function (err, ary) {
      if(err) throw err
      console.log(ary)
      ary.forEach(function (e) {
        t.ok(compare(e, target) >= 0)
      })
      t.end()
    })
  )
})

