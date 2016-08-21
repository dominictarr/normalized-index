
var tape = require('tape')

var Offset = require('offset-log')
var Index = require('../')
var IndexTable = require('../table')

var mkdirp = require('mkdirp')

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = Offset(dir+'/log')
console.log(dir)

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var index = Index(log, compare, decode)

function encode (value) {
  return new Buffer(JSON.stringify(value))
}

function decode (value) {
  return JSON.parse(value.toString())
}

tape('simple', function (t) {
  log.append(encode({key: 'ABC', seq: 1}), function (err, _offset) {
    if(err) throw err
    index.search({key: 'ABC'}, function (err, value, offset, i) {
      if(err) throw err
      t.equal(offset, _offset)
      t.equal(i, 0)
      t.end()
    })
  })
})

tape('more', function (t) {
  log.append(encode({key: 'LMN', seq: 2}), function (err, offset1) {
    if(err) throw err
    log.append(encode({key: 'XYZ', seq: 3}), function (err, offset2) {
      index.search({key: 'LMN'}, function (err, value, offset, i, exact) {
        if(err) throw err
        console.log(offset, value)
        t.equal(offset, offset1, 'offsets are equal')
        t.equal(value.key, 'LMN')
        console.log('offset', offset)
        t.equal(exact, true)
        log.get(offset, function (err, _value) {
          t.deepEqual(value, decode(_value))
          t.end()
        })
      })
    })
  })
})

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
    if(i === a.length) t.end()
    else log.append(encode(a[i++]), loop)
  })()

})

tape('search alphabet', function (t) {

  ;(function next () {
    var i = 0
    ;(function loop () {
      if(i === a.length) return t.end()
      console.log('search for:', target)
      var target = a[i++]
      index.search(target, function (err, value) {
        t.deepEqual(value, target)
        loop()
      })
    })()
  })()
})

tape('serialize', function (t) {
  var table = IndexTable(index.serialize(), log, compare, decode)

  ;(function next () {
    var i = 0
    ;(function loop () {
      if(i === a.length) return t.end()
      var target = a[i++]
      console.log('search for:', target)
      table.search(target, function (err, value, _, __, exact) {
        t.deepEqual(value, target)
        t.equal(exact, true)
        loop()
      })
    })()
  })()
})

tape('partial', function (t) {
  var target = {key:'pq'}
  index.search(target, function (err, value, offset, i, exact) {
    if(err) throw err
    console.log(offset, value)
    t.equal(exact, false)
    t.ok(compare(value, target) < 0, 'search returns value before target')
    index.get(~i+1, function (err, _value, _offset, j) {
      if(err) throw err
      console.log(_offset, _value)
      t.ok(compare(_value, target) > 0, 'get i+1 value after target')
      t.equal(~i + 1, j)
      t.end()
    })
  })
})

tape('out of bounds, high', function (t) {

  index.search({key: 'zzz'}, function (err, value, offset, i, exact) {
    console.log('index', ~i, index.length())
    t.equal(~i, index.length()-1)
    console.log(err, value, offset, i)
    t.end()
  })
})


tape('out of bounds, low', function (t) {
  index.search({key: '!'}, function (err, value, offset, i, exact) {
    t.equal(i, -1)
    console.log(err, value, offset, i)
    t.end()
  })
})

