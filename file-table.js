'use strict'
var search = require('binary-search-async')
var Blocks = require('aligned-block-file')

module.exports = function (file, log, compare) {
  var blocks = Blocks(file, 1024)
  var max = 0

  function offset(index, cb) {
    if(isNaN(index)) throw new Error('index was NaN')
    blocks.readUInt32BE(4+index*4, cb)
  }

  function get (i, cb) {
    offset(i, function (err, key) {
      if(err) return cb(err)
      log.get(key, function (err, value) {
        if(err) return cb(err)
        cb(null, value, key)
      })
    })
  }

  var self
  return self = {
    get: get,
    length: function () {
      return (blocks.size()-4)/4
    },
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, self.length()-1, function (err, i, value) {
        if(err) return cb(err)
        if(i < 0) {
          cb(err, value, null, i)
        } else {
          offset(i, function (err, key) {
            cb(err, value, key, i)
          })
        }
      })
    }
  }
}










