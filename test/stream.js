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

function all(opts, cb) {
  pull(Stream(index, opts), pull.collect(cb))
}

function test(name, opts, fn) {
  tape(name, function (t) {
    all(opts, function (err, ary) {
      if(err) throw err
      fn(t, ary)
      opts.keys = true
      all(opts, function (err, with_keys) {
        if(err) throw err
        var vals = with_keys.map(function (e) { return e.value })
        t.deepEqual(vals, ary)
        fn(t, vals)
        opts.values = false
        all(opts, function (err, keys) {
          t.deepEqual(keys, with_keys.map(function (e) { return e.key }))

          opts.values = true
          opts.keys = false
          opts.reverse = true
          all(opts, function (err, reverse_ary) {
            if(err) throw err
            fn(t, reverse_ary)
            t.deepEqual(reverse_ary, ary.slice().reverse(), 'output correctly reversed')
            t.end()
          })
        })
      })
    })
  })
}

test('everthing', {}, function (t, ary) {
  console.log(ary)
  t.equal(ary.length, 24)
//  t.deepEqual(ary, ary.slice().sort(compare))
})

//test('everything, reversed', {reverse: true}, function (t, ary) {
//  t.equal(ary.length, 24)
//  t.deepEqual(ary, ary.slice().sort(compare).reverse())
//})

var i = ~~(Math.random() * a.length)
var target = a[i]

test('stream to half-way', {gte: target}, function (t, ary) {
  ary.forEach(function (e) {
    t.ok(compare(e, target) >= 0,e.key + ' >= ' +target.key)
  })
})

for(var n = 0; n < 10; n++) (function () {

  var x = ~~(Math.random()*a.length)
  var y = ~~(Math.random()*a.length)
  var start = a[Math.min(x, y)]
  var end = a[Math.max(x, y)]

  test('stream middle range:'+start.key +'<'+end.key, {gt: start, lt: end}, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) > 0, e.key+'>'+start.key)
      t.ok(compare(e, end) < 0, e.key+'<'+end.key)
    })
  })

  test('stream middle range, end inclusive:'+start.key +'<='+end.key, {gt: start, lte: end}, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) > 0, e.key+'>'+start.key)
      t.ok(compare(e, end) <= 0, e.key+'<='+end.key)
    })
  })

  test('stream middle range, start inclusive:'+start.key +'=<'+end.key,
  {gte: start, lt: end}, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) >= 0, e.key+'>='+start.key)
      t.ok(compare(e, end) < 0, e.key+'<'+end.key)
    })
  })

})()

