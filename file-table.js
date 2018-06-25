'use strict'
var search = require('binary-search-async')
var Blocks = require('aligned-block-file')

/*
  sorted index stored in a binary file.
  the main database should be a series of these.
*/

module.exports = function (file, log, compare, cb) {
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
    filename: file,
    get: get,
    ready: function (cb) {
      blocks.offset.once(function () { cb() })
    },
    length: function () {
      return (blocks.size()-4)/4
    },
    range: function (start, end, cb) {
      if(start > end) return cb(null, [])
      blocks.read(4+start*4, 4+end*4+4, cb)
    },
    search: function (target, cb) {
      //we need to know the maximum value
      blocks.offset.once(function (off) {
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
      })
    }
  }
}

