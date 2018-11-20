'use strict'
var search = require('binary-search-async')
var Blocks = require('aligned-block-file')
var seek = require('binary-search-async/seek')
var Stream = require('./stream')
/*
  sorted index stored in a binary file.
  the main database should be a series of these.
*/

module.exports = function (file, log, compare, cb) {
  var blocks = Blocks(file, 1024)
  var max = 0, latest

  function offset(index, cb) {
    if(isNaN(index)) throw new Error('index was NaN')
    blocks.readUInt32BE(4+index*4, cb)
  }

  var cache = require('hashlru')(1024)

  function get (i, cb) {
    var data = cache.get(i)
    if(data) {
      if(Array.isArray(data))
        data.push(cb)
      else {
        cb(null, data.value, data.key)
      }
    }
    else {
      var waiting = [cb]
      cache.set(i, waiting)
      offset(i, function (err, key) {
        if(err) return cb(err)
        log.get(key, function (err, value) {
          if(err) return cb(err)

          cache.set(i, {value:value, key: key})
          while(waiting.length) waiting.shift()(null, value, key)
        })
      })
    }
  }

  var self
  return self = {
    filename: file,
    compare: compare,
    get: get,
    ready: function (cb) {
      blocks.offset.once(function () {
        blocks.readUInt32BE(0, function (_, _latest) {
          latest = _latest
          cb()
        })
      })
    },
    length: function () {
      return (blocks.size()-4)/4
    },
    latest: function () {
      return latest
    },
    range: function (start, end, cb) {
      if(start > end) return cb(null, [])
      blocks.read(4+start*4, 4+end*4+4, cb)
    },
    seek: function (target, start, cb) {
      start = start | 0
      blocks.offset.once(function (off) {
        seek(self.get, target, compare, start, 0, self.length()-1, cb)
      })
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
    },
    stream: function (opts) {
      return Stream(this, opts)
    }
  }
}

