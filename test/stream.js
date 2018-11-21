var tape = require('tape')

var FlumeLog = require('flumelog-offset')
var Index = require('../memory')
var IndexTable = require('../table')
var IndexFile = require('../file-table')
var pull = require('pull-stream')
var mkdirp = require('mkdirp')
var fs = require('fs')
var ltgt = require('ltgt')

var MergeStream = require('../merge-stream')

function Merged () {
  var indexes = [].slice.call(arguments)
  return {
    stream: function (opts) {
      return MergeStream(indexes, opts)
    },
    indexes: indexes
  }
}

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = FlumeLog(dir+'/log', {codec: require('flumecodec/json')})

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : a.random - b.random
}
var index = Index(compare)
var index2, fileTable
pull(
  log.stream({live: true, keys: true, values: true}),
  pull.drain(function (data) {
    index.add({key:data.seq, value: data.value})
    if(index2)
      index2.add({key:data.seq, value: data.value})
  })
)

var table, table2

var alpha = 'abcdefghijklmnopqrstuvwxyz'
var a = [], j = 0
for(var i = 0; i + 2 < alpha.length; i++) {
  do {
    var value = {
      key: alpha[i] + alpha[i+1] + alpha[i+2],
      seq: j++,
      random: Math.random()
    }
    a.push(value)
  } while (value.random < 0.1)
}

a.sort(compare)

tape('alphabet', function (t) {

  //sort randomly
  a.sort(function () {
    return Math.random() - 0.5
  })

  var N = 10 + ~~(Math.random()*10)
  var i = 0
  ;(function loop () {
    if(i === N) {
      table2 = IndexTable(index.serialize(), log, compare)
      index2 = Index(compare)
    }
    if(i === a.length) t.end()
    else log.append(a[i++], loop)
  })()

})

tape('create table', function (t) {
  var buf = index.serialize()
  table = IndexTable(buf, log, compare)
  var file = dir+'/table'
  fs.writeFileSync(file, buf)
  fileTable = IndexFile(file, log, compare)

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
  pull(index.stream(opts), pull.collect(cb))
}

function error(err) {
  if(err.stack) return err
  return new Error('error without stack'+JSON.stringify(err))
}

function diff(actual, expected) {
  var missing = []
  expected.forEach(function (a) {
    var b = actual.find(function (b) {
      return compare(a, b) === 0
    })
    if(!b) missing.push(a)
  })
  return missing
}

function missing (t, actual, expected) {
  t.deepEqual(diff(actual, expected), [], 'values were missing from output')
  t.deepEqual(diff(expected, actual), [], 'extra values in output')
}

function test(name, _opts, fn) {
  var expected = a.filter(function (e) {
    return ltgt.contains(_opts, e, compare)
  })
  tape(name + ' '  + JSON.stringify(_opts), function (t) {
    console.log('TEST', _opts)
    function tests (n, index) {
      var opts = {}
      for(var k in _opts)
        opts[k] = _opts[k]

      t.test(n+name, function (t) {
        all(index, opts, function (err, ary) {
          if(err) throw error(err)
          missing(t, ary, expected)
          t.equal(ary.length, expected.length)
//          t.deepEqual(ary.sort(compare), expected) //opts.reverse ? expected.reverse() : expected)
          t.deepEqual(ary, expected) //opts.reverse ? expected.reverse() : expected)

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
    tests('file:', fileTable)
    tests('merge:', Merged(table2, index2))

  })
}

test('everthing', {}, function (t, ary) {
  t.equal(ary.length, a.length)
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

  var first = a[0]
  var last = a[a.length-1]

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

  function assertRange(t, ary, range) {
    ary.forEach(function (e) {
      t.ok(range(e), JSON.stringify(e) + ' is within range')
    })
    t.deepEqual(ary.filter(range).length, a.filter(range).length)
    t.deepEqual(ary.filter(range), a.filter(range)
      .sort(compare))
  }

  var s_part = { key: start.key.substring(0, 2)}
  var e_part = {key: end.key + '~'}

  test('stream part middle range, start inclusive:'+s_part.key +'=<'+end.key,
  opts = { gte: s_part, lt:  end }, function (t, ary) {
    t.ok(ary.every(function (e) {
      if(!(compare(e, s_part) >= 0)) throw new Error(JSON.stringify(e)+'>='+JSON.stringify(start))
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

  test('stream just first: =<'+first.key,
  { lte: first}, function () {})
  test('stream just last: =<'+first.key,
  { gte: last}, function () {})

  var l_part = last.key.substring(0, 2)
  test('stream just last: >='+l_part,
  { gte: {key: l_part}}, function () {})

  test('stream eq:'+start.key, {gte: {key:start.key, random: 0}, lte: {key:start.key, random: 1}}, 
    function (t, ary){
      console.log(ary)
      t.ok(ary.length) //1 or more
      t.equal(ary[0].key, start.key)
      ary.every(function (e) {
        t.equal(e.key, start.key)
      })
    })

//if you pass an object with undefined keys, should it
//automatically insert top and bottom range values when searching?
//i'm gonna say: no.

//  test('stream eq:'+start.key, {gte: {key:start.key}, lte: {key:start.key}}, 
//    function (t, ary){
//      console.log(ary)
//      t.ok(ary.length) //1 or more
//      t.equal(ary[0].key, start.key)
//      ary.every(function (e) {
//        t.equal(e.key, start.key)
//      })
//    })

})()

