'use strict'
module.exports = function (index, opts) {
  opts = opts || {}
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
          l_index = i + l_incl; ready()
        })
      if(upper !== undefined)
        index.search(upper, function (err, _, __, i, exact) {
          if(error) return; if(err) return cb(error = err)
          u_index = i + u_incl - (exact ? 0 : 1);

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

