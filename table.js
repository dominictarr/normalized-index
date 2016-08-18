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
        cb(null, value)
      })
    })
  }
  return {
    get: get,
    length: function () { return max },
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, max-1, function (err, value, idx, exact) {
        idx = Math.min(idx, max-1)
        cb(err, value, offset(idx), idx, exact)
      })
    }
  }

}







