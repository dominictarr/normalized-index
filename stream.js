'use strict'
var Merge = require('pull-merge')
var pull = require('pull-stream/pull')
var Map = require('pull-stream/throughs/map')

function flip(cmp) {
  return function (a, b) {
    return cmp(a, b)*-1
  }
}

module.exports = function Stream (index, opts, compare) {
  opts = opts || {}
  if(Array.isArray(index)) {

    // if index is an array of indexes, search all of them and merge.
    // this is correct, but likely not optimal in the case of
    // Log Structered Merge. I figure that a LSM merge likely has one index
    // much larger than another ("sparse merge") so you could search ahead to find i: B[i] < A[j]
    // then output the intermediate keys (since an LSM of a normalized index only needs keys)

    var keys = opts.keys === true
    var values = opts.values !== false
    opts.keys = true
    opts.values = true
    return pull(
      Merge(index.map(function (i) { return Stream(i, opts) }), function (a, b) {
        return compare(a, b) * (opts.reverse ? -1 : 1)
      }),
      Map(function (e) {
        return keys && values ? e : keys ? e.key : e.value
      })
    )

  }

  var lower, upper, l_index, u_index
  var u_incl, l_incl
  var error

  if(upper = opts.max || opts.lte) u_incl = 0
  else if(upper = opts.lt)         u_incl = -1

  if(lower = opts.min || opts.gte) l_incl = 0
  else if(lower = opts.gt)         l_incl = 1

  if(upper === undefined) u_index = index.length() - 1
  if(lower === undefined) l_index = 0

  var keys = opts.keys === true
  var values = opts.values !== false

  function next(cb) {
    if(u_index < l_index) return cb(true)
    var i = opts.reverse ? u_index-- : l_index++
    index.get(i, function (err, value, key) {
      if(err) return cb(err)

      cb(null,
          keys && values ? {key: key, value: value}
        : keys           ? key
        :                  value
      )
    })
  }

  return function (abort, cb) {
    if(abort || error) return cb(abort || error)
    else if(l_index === undefined || u_index === undefined) {
      //if we have a index bounded at both ends,
      //search for both ends, then just iterate between them.
      if(lower !== undefined)
        index.search(lower, function (err, _, __, i) {
          if(error) return; if(err) return cb(error = err)
          l_index = (i < 0 ? ~i + 1 : i)  + l_incl;
          l_index = Math.max(l_index, 0)
          if(~i === index.length()-1)
            l_index ++
          ready()
        })
      if(upper !== undefined)
        index.search(upper, function (err, _, __, i) {
          if(error) return; if(err) return cb(error = err)
          u_index = (i < 0 ? ~i - 1 : i) + u_incl
          u_index = Math.min(u_index, index.length() - 1)
          ready()
        })
      function ready () {
        if(l_index === undefined || u_index == undefined) return
        next(cb)
      }
    }
    else
      next(cb)
  }
}

