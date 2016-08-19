'use strict'
var search = require('binary-search-async')
var pull = require('pull-stream')

//this is an in-memory index, must be rebuilt from the log.
module.exports = function (log, compare, decode, old) {

  //read the current max from the end of the start of the index file.

  var index = [], sorted = false
  var max = 0
  pull(
    log.stream({live: true, old: old, sync: false, keys: true}),
    pull.drain(function (data) {
      max = data.key
      sorted = false
      index.push({key: data.key, value: decode(data.value)})
    })
  )

  function sort () {
    if(!sorted) {
      index.sort(function cmp (a, b) {
        return compare(a.value, b.value) || a.key - b.key
      })
      sorted = true
    }
  }

  function get (i, cb) {
    sort()
    if(i < 0 || i >= index.length)
      cb(new Error('out of bounds'))
    else
      cb(null, index[i].value, index[i].key, i)
  }
  var self
  return self = {
    length: function () { return index.length },
    get: get,
    search: function (target, cb) {
      sort()
      //we need to know the maximum value
      search(get, target, compare, 0, index.length - 1, function (err, value, idx, exact) {
        var key
        if(idx >= 0) {
          idx = Math.min(idx, index.length - 1)
          key = index[idx].key
        }
        cb(err, value, key, idx, exact)
      })
    },
    serialize: function () {
      sort()
      //format is <max_offset><ordered_offsets+>
      var b = new Buffer((index.length+1)*4)
      b.writeUInt32BE(max, 0)
      index.forEach(function (e, i) {
        b.writeUInt32BE(e.key, 4+i*4)
      })
      return b
    }
  }
}



