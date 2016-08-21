'use strict'
var search = require('binary-search-async')
var Blocks = require('block-reader')

module.exports = function (file, log, compare, decode) {
  console.log(file)
  var blocks = Blocks(file, 1024)
  var max = 0

  function offset(index, cb) {
    blocks.readUInt32BE(4+index*4, cb)
  }

  function get (i, cb) {
    setImmediate(function () {
      offset(i, function (err, key) {
        if(err) return cb(err)
        log.get(key, function (err, value) {
          if(err) return cb(err)
          value = decode(value)
          cb(null, value, key)
        })
      })
    })
  }

  var self
  return self = {
    get: get,
    length: function () {
      return (blocks.size()/4) - 1
    },
    search: function (target, cb) {
      //we need to know the maximum value
      search(get, target, compare, 0, self.length(), function (err, value, i, exact) {
        if(err) return cb(err)
        if(i < 0)
          cb(err, value, null, i, exact)
        else {
          offset(i, function (err, key) {
            cb(err, value, key, i, exact)
          })
        }
      })
    }
  }
}














