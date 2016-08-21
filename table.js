'use strict'
var search = require('binary-search-async')
module.exports = function (table, log, compare, decode) {
  function offset(index) {
    return table.readUInt32BE(4+index*4)
  }
  var max = table.length/4 - 1

  function get (i, cb) {
    setImmediate(function () {
      log.get(offset(i), function (err, value) {
        value = decode(value)
        cb(null, value, offset(i))
      })
    })
  }
  return {
    get: get,
    length: function () { return max },
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, max, function (err, value, idx, exact) {
        cb(err, value, idx >= 0 ? offset(idx) : null, idx, exact)
      })
    }
  }
}



