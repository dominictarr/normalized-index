'use strict'
var _seek = require('binary-search-async/seek')
module.exports = function (a, b, compare) {

  var a_value, b_value, a_i = 0, b_i = 0, first = true, ended, a_ended, b_ended

  function swap () {
    var t = a, t_i = a_i, t_value = a_value
    a_i = b_i; a_value = b_value
    b_i = t_i; b_value = t_value
    a = b; b = t
    var t_ended = a_ended
    a_ended = b_ended; b_ended = t_ended
  }

  function seek(x, value, i, cb) {
    _seek(x.get, value, compare, i, 0, x.length()-1, cb)
  }

  function more (cb) {
    if(a_ended && b_ended) {
      return cb(ended = true)
    }
    else if(a_ended) {
      //if a has ended, there must be one last range in b.
      b_ended = true
      b.range(b_i, b.length()-1, cb)
    }
    else
      return seek(b, a_value, b_i, function (err, i, value) {
        if(err) return cb(err)
        return b.range(b_i, i-1, function (err, range) {
          if(err) return cb(err)
          b_i = i
          b_value = value
          if(b_i > b.length()-1) b_ended = true
          swap()
          setImmediate(function () {
            cb(null, range)
          })
        })
      })
  }

  return function (abort, cb) {
    if(ended) cb(ended)
    else if(first) {
      first = false
      a.get(0, function (err, value) { a_value = value; next(err) })
      b.get(0, function (err, value) { b_value = value; next(err) })
      function next (err) {
        if(ended = ended || err) cb(err)
        else if(a_value != null && b_value != null) {
          var c = compare(a_value, b_value)
          //a is smaller
          if(c < 0) swap()
          more(cb)
        }
      }
    }
    else more(cb)
  }
}

if(!module.parent) {
  function compare (a, b) { return a - b }
  var k = 0
  function create (name, N, initial, mult) {
    var ary = [initial]
    for(var i = 0; i < N; i++)
      ary.push(Math.random()*(mult | 0))
    ary.sort(compare)
  //  console.log(name, ary)
    return {
      array: ary,
      get: function (i, cb) {
        k++
        return cb(null, ary[i])
      },
      range: function (start, end, cb) {
        cb(null, ary.slice(start, end+1))
      },
      length: function () { return ary.length }
    }
  }

  console.log("TEST")
  var pull = require('pull-stream')
  var N = 1000
  var a = create('A', N, 1, 50)
  var b = create('B', N, 0, 25)
  pull(
    module.exports(a, b,compare),
  //  pull.through(function (e) { console.log (e.length) }),
    pull.collect(function (err, ary) {
      var cat = [].concat.apply([], ary)
      console.log(cat.length, k)
    })
  )
  k = 0
  pull(
  //  require('./sparse-merge')
    module.exports
    (a, b,compare),
  //  pull.through(function (e) { console.log (e.length) }),
    pull.collect(function (err, ary) {
      var cat = [].concat.apply([], ary)
      console.log(cat.length, k)
    })
  )
}
