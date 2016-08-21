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
var table, table2

var alpha = 'abcdefghijklmnopqrstuvwxyz'
var a = []
for(var i = 0; i + 2 < alpha.length; i++)
  a.push({key: alpha[i] + alpha[i+1] + alpha[i+2], seq: i+10})

tape('alphabet', function (t) {

  //sort randomly
  a.sort(function () {
    return Math.random() - 0.5
  })

  var N = 10 + ~~(Math.random()*10)
  var i = 0
  ;(function loop () {
    if(i === N) {
      table2 = IndexTable(index.serialize(), log, compare, decode)
      index2 = Index(log, compare, decode, false)
    }
    if(i === a.length) t.end()
    else log.append(encode(a[i++]), loop)
  })()

})

tape('create table', function (t) {
  table = IndexTable(index.serialize(), log, compare, decode)

  console.log('index.length', index.length())
  console.log('table.length', table.length())
  console.log('index2.length', index2.length())
  console.log('table2.length', table2.length())

  all(table2, {}, function (err, ary) {
    console.log('table2', ary)
    all(index2, {}, function (err, ary) {
      console.log('index2', ary)
      t.end()
    })
  })
})


function all(index, opts, cb) {
  pull(Stream(index, opts, compare), pull.collect(cb))
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

            t.ok(with_keys.every(function (e) {
              return ~~e.key == e.key //is integer
            }), 'keys are integers')

          fn(t, vals)
            opts.values = false
            all(index, opts, function (err, keys) {
              t.deepEqual(keys, with_keys.map(function (e) { return e.key }))
              t.ok(keys.every(function (e) {
                return ~~e == e //is integer
              }), 'keys are integers')

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
    tests('merge:', [table2, index2])
  })
}

test('everthing', {}, function (t, ary) {
  t.equal(ary.length, 24)
})

var i = ~~(Math.random() * a.length)
var target = a[i]

function assertEmpty(t, ary) {
  if(!ary.length) return
  t.ok(false, 'should be empty:'+JSON.stringify(ary))
}

test('empty stream, before start, closed', {gt: {key:'!'}, lt: {key:'!!'}}, assertEmpty)
test('empty stream, before start, open', {lt: {key:'!!'}}, assertEmpty)
test('empty stream, lte !! start, open', {lte: {key:'!!'}}, assertEmpty)
test('empty stream, before after end, closed', {gt: {key:'~'}, lt: {key:'~~'}}, assertEmpty)
test('empty stream, before after end, open', {gt: {key:'~'}}, assertEmpty)
test('empty stream, >= end, open', {gte: {key:'~'}}, assertEmpty)


test('stream to half-way : >='+target.key, {gte: target}, function (t, ary) {
  console.log(ary, target)
  t.ok(ary.every(function (e) {
    if(!(compare(e, target) >= 0)) throw new Error(e.key+'>='+target.key)
    return true
  }))

})

var min = {key: '!'}, max = {key: '~'}
test('stream part middle range, start inclusive:!=<~',
{ gte: min, lt: max}, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, min) >= 0)) throw new Error(e.key+'>='+min.key)
      if(!(compare(e, max) < 0)) throw new Error(e.key+'<'+max.key)
      return true
    }))
})

for(var n = 0; n < 10; n++) (function () {

  var x = ~~(Math.random()*a.length)
  var y = ~~(Math.random()*a.length)
  var start = a[Math.min(x, y)]
  var end = a[Math.max(x, y)]

  test('stream middle range:'+start.key +'<'+end.key, {gt: start, lt: end}, function (t, ary) {
    t.ok(ary.every(function (e) {
      return compare(e, start) > 0 && compare(e, end) < 0
    }), 'within range')
  })

  test('stream middle range, end inclusive:'+start.key +'<='+end.key, {gt: start, lte: end}, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, start) > 0)) throw new Error(e.key+'>'+start.key)
      if(!(compare(e, end) <= 0)) throw new Error(e.key+'<='+end.key)
      return true
    }))
  })

  test('stream middle range, start inclusive:'+start.key +'=<'+end.key,
  {gte: start, lt: end}, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, start) >= 0)) throw new Error(e.key+'>='+start.key)
      if(!(compare(e, end) < 0)) throw new Error(e.key+'<'+end.key)
      return true
    }))
  })

  var s_part = { key: start.key.substring(0, 2)}
  var e_part = {key: end.key + '~'}

  test('stream part middle range, start inclusive:'+s_part.key +'=<'+end.key,
  { gte: s_part, lt:  end }, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, start) >= 0)) throw new Error(e.key+'>='+start.key)
      if(!(compare(e, end) < 0)) throw new Error(e.key+'<'+end.key)
      return true
    }))
  })

  test('stream part middle range, end inclusive:'+s_part.key +'=<'+end.key,
  { gt: s_part, lte:  end }, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, s_part) > 0)) throw new Error(e.key+'>'+s_part.key)
      if(!(compare(e, end) <= 0)) throw new Error(e.key+'<='+end.key)
      return true
    }))
  })

  test('stream part middle range, end inclusive:'+start.key +'<='+e_part.key,
  { gt: start, lte:  e_part }, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, start) > 0)) throw new Error(e.key+'>'+start.key)
      if(!(compare(e, e_part) <= 0)) throw new Error(e.key+'<='+e_part.key)
      return true
    }))

  })

  test('stream part middle range, start inclusive:'+start.key +'=<'+e_part.key,
  { gte: start, lt: e_part }, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, start) >= 0)) throw new Error(e.key+'>='+start.key)
      if(!(compare(e, e_part) < 0)) throw new Error(e.key+'<'+e_part.key)
      return true
    }))
  })
})()











