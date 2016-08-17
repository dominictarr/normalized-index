
var tape = require('tape')


var Offset = require('offset-log')
var Index = require('../')

var mkdirp = require('mkdirp')

var dir = '/tmp/test-normalized-index_'+Date.now()
mkdirp.sync(dir)

var log = Offset(dir+'/log')
console.log(dir)
var index = Index(null, log, function (a, b) {
//  console.log('compare',a,b, a.key < b.key ? -1 : a.key > b.key ? 1 : 0)
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}, decode)

function encode (value) {
  return new Buffer(JSON.stringify(value))
}
function decode (value) {
  return JSON.parse(value.toString())
}

tape('simple', function (t) {
  log.append(encode({key: 'ABC', seq: 1}), function (err, _offset) {
    if(err) throw err
    index.search({key: 'ABC'}, function (err, offset, value) {
      if(err) throw err
      t.equal(offset, _offset)
      t.end()
    })
  })
})

tape('more', function (t) {
  log.append(encode({key: 'LMN', seq: 2}), function (err, offset1) {
    if(err) throw err
    log.append(encode({key: 'XYZ', seq: 3}), function (err, offset2) {
      index.search({key: 'LMN'}, function (err, offset, value) {
        if(err) throw err
        console.log(offset, value)
        t.equal(offset, offset1, 'offsets are equal')
        t.equal(value.key, 'LMN')
        console.log('offsuet', offset)
        log.get(offset, function (err, _value) {
          t.deepEqual(value, decode(_value))
          t.end()
        })
      })
    })
  })
})

tape('alphabet', function (t) {
  var alpha = 'abcdefghijklmnopqrstuvwxyz'
  var a = []
  for(var i = 0; i + 3 < alpha.length; i++)
    a.push({key: alpha[i] + alpha[i+1] + alpha[i+2], seq: i})

  //sort randomly
  a.sort(function () {
    return Math.random() - 0.5
  })

  var i = 0
  ;(function loop () {
    if(i === a.length) next()
    else
      log.append(encode(a[i++]), function (err, offset) {
        loop()
      })
  })()

  function next () {
    var i = 0
    ;(function loop () {
      if(i === a.length) return t.end()
      console.log('search for:', target)
      var target = a[i++]
      index.search(target, function (err, offset, value) {
//        console.log(offset, value)
        t.deepEqual(value, target)
        loop()
      })
    })()
  }
})


