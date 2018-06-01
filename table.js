'use strict'
var search = require('binary-search-async')
module.exports = function (table, log, compare) {
  if(!Buffer.isBuffer(table)) throw new Error('table should be a buffer')

  function offset(index) {
    return table.readUInt32BE(4+index*4)
  }

  var max = (table.length-4)/4 - 1

  function get (i, cb) {
    setImmediate(function () {
      log.get(offset(i), function (err, value) {
        cb(null, value, offset(i))
      })
    })
  }
  return {
    get: get,
    length: function () { return max + 1},
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, max, function (err, idx, value) {
        cb(err, value, idx >= 0 ? offset(idx) : null, idx, idx >= 0)
      })
    }
  }
}



