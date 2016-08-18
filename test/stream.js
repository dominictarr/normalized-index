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
var table

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

  console.log(index.serialize())

})

tape('create table', function (t) {
  table = IndexTable(index.serialize(), log, compare, decode)
  t.end()
})


function all(index, opts, cb) {
  pull(Stream(index, opts), pull.collect(cb))
}

function error(err) {
  if(err.stack) return err
  return new Error('error without stack'+JSON.stringify(err))
}

function test(name, _opts, fn) {
  tape(name, function (t) {
    function tests (n, index) {
      var opts = {}
      for(var k in _opts)
        opts[k] = _opts[k]

      t.test(n+name, function (t) {
        all(index, opts, function (err, ary) {
          if(err) throw error(err)
          fn(t, ary)
          opts.keys = true
          all(index, opts, function (err, with_keys) {
            if(err) throw error(err)
            var vals = with_keys.map(function (e) { return e.value })
            t.deepEqual(vals, ary)
            fn(t, vals)
            opts.values = false
            all(index, opts, function (err, keys) {
              t.deepEqual(keys, with_keys.map(function (e) { return e.key }))

              opts.values = true
              opts.keys = false
              opts.reverse = true
              all(index, opts, function (err, reverse_ary) {
                if(err) throw error(err)
                fn(t, reverse_ary)
                t.deepEqual(reverse_ary, ary.slice().reverse(), 'output correctly reversed')
                t.end()
              })
            })
          })
        })
      })
    }
    tests('mem:', index)
    tests('table:', table)
  })
}

test('everthing', {}, function (t, ary) {
  console.log(ary)
  t.equal(ary.length, 24)
})

var i = ~~(Math.random() * a.length)
var target = a[i]

test('stream to half-way', {gte: target}, function (t, ary) {
  ary.forEach(function (e) {
    t.ok(compare(e, target) >= 0,e.key + ' >= ' +target.key)
  })
})

var min = {key: '!'}, max = {key: '~'}
test('stream part middle range, start inclusive:!=<~',
{ gte: min, lt: max}, function (t, ary) {
  ary.forEach(function (e) {
    t.ok(compare(e, min) >= 0, e.key+'>'+min.key)
    t.ok(compare(e, max) < 0, e.key+'<'+max.key)
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

  var s_part = { key: start.key.substring(0, 2)}
  var e_part = {key: end.key + '~'}

  test('stream part middle range, start inclusive:'+s_part.key +'=<'+end.key,
  { gte: s_part, lt:  end }, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) >= 0, e.key+'>='+s_part.key)
      t.ok(compare(e, end) < 0, e.key+'<'+end.key)
    })
  })

  test('stream part middle range, end inclusive:'+s_part.key +'=<'+end.key,
  { gt: s_part, lte:  end }, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) > 0, e.key+'>'+s_part.key)
      t.ok(compare(e, end) <= 0, e.key+'<'+end.key)
    })
  })

  test('stream part middle range, end inclusive:'+start.key +'<='+e_part.key,
  { gt: start, lte:  e_part }, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) > 0, e.key+'>'+start.key)
      t.ok(compare(e, end) <= 0, e.key+'<'+e_part.key)
    })
  })

  test('stream part middle range, start inclusive:'+start.key +'=<'+e_part.key,
  { gte: start, lt: e_part }, function (t, ary) {
    ary.forEach(function (e) {
      t.ok(compare(e, start) >= 0, e.key+'>'+start.key)
      t.ok(compare(e, end) < 0, e.key+'<'+e_part.key)
    })
  })

})()





