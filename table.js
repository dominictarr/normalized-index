'use strict'
/*
  sorted table, in a memory buffer.
  this was intended for testing and to get the code right,
  not actual use. See file-table instead.

*/

var search = require('binary-search-async')
var seek = require('binary-search-async/seek')

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
    compare: compare,
    get: get,
    length: function () { return max + 1},
    range: function (start, end, cb) {
      cb(null, table.slice(4+start*4, 8+end*4))
    },
    seek: function (target, start, cb) {
      seek(get, target, compare, start, 0, max, cb)
    },
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, max, function (err, idx, value) {
        cb(err, value, idx >= 0 ? offset(idx) : null, idx)
      })
    }
  }
}



