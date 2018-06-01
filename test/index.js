
var tape = require('tape')
var pull = require('pull-stream')

var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var Index = require('../')
var IndexTable = require('../table')

var mkdirp = require('mkdirp')

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = FlumeLog(dir+'/log', {codec: json})

function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var index = Index(/*log, */compare, decode)

pull(
  log.stream({live: true}),
  pull.drain(function (data) {
    index.add({key:data.seq, value: data.value})
  })
)

function _id (e) { return e}
var encode = _id
var decode = _id

tape('simple', function (t) {
  var abc = {key: 'ABC', value: Math.random()}
  log.append(abc, function (err, _offset) {
    if(err) throw err
    index.search({key: 'ABC'}, function (err, value, offset, i) {
      if(err) throw err
      console.log(value, offset, i)
      t.equal(offset, _offset)
      t.equal(i, 0)
      t.deepEqual(value, abc)
      t.end()
    })
  })
})

tape('more', function (t) {
  var lmn = {key: 'LMN', random: Math.random()}
  var xyz = {key: 'XYZ', random: Math.random()}
  log.append(lmn, function (err, offset1) {
    if(err) throw err
    log.append(xyz, function (err, offset2) {
      index.search({key: 'LMN'}, function (err, value, offset, i) {
        if(err) throw err
        console.log(offset, value)
        t.equal(offset, offset1, 'offsets are equal')
        t.equal(value.key, 'LMN')
        console.log('offset', offset)
        log.get(offset, function (err, _value) {
          t.deepEqual(value, _value)
          t.end()
        })
      })
    })
  })
})

var alpha = 'abcdefghijklmnopqrstuvwxyz'
var a = []
for(var i = 0; i + 2 < alpha.length; i++)
  a.push({key: alpha[i] + alpha[i+1] + alpha[i+2], index: i+10})

tape('alphabet', function (t) {

  //sort randomly
  a.sort(function () {
    return Math.random() - 0.5
  })

  var i = 0
  ;(function loop (err) {
    if(err) throw err
    if(i === a.length) t.end()
    else log.append(a[i++], loop)
  })()

})

tape('search alphabet', function (t) {

  ;(function next () {
    var i = 0
    ;(function loop () {
      if(i === a.length) return t.end()
      var target = a[i++]
      console.log('search for:', target)
      index.search(target, function (err, value, i) {
        console.log(err, value, i)
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
        if(err) throw err
        t.deepEqual(value, target)
//        t.equal(exact, true)
        loop()
      })
    })()
  })()
})

tape('partial', function (t) {
  var target = {key:'pq'}
  index.search(target, function (err, value, offset, i) {
    if(err) throw err
    console.log(offset, value)
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
    console.log('index', ~i, index.length(), value, offset)
    t.equal(~i, index.length())
    console.log(err, value, offset, i)
    t.end()
  })
})

tape('out of bounds, inbetween', function (t) {
  index.search({key: 'xxx'}, function (err, value, offset, i, exact) {
    console.log('index', ~i, index.length(), value, offset)
    t.equal(~i, index.length()-1)
    console.log(err, value, offset, i)
    t.end()
  })
})
tape('out of bounds, inbetween', function (t) {
  index.search({key: 'www'}, function (err, value, offset, i, exact) {
    console.log('index', ~i, index.length(), value, offset)
    t.equal(~i, index.length()-2)
    t.end()
  })
})

tape('out of bounds, low', function (t) {
  index.search({key: '!'}, function (err, value, offset, i, exact) {
    t.equal(i, -1)
    t.equal(~i, 0)
    console.log(err, value, offset, i)
    t.end()
  })
})

