'use strict'
var search = require('binary-search-async')
var pull = require('pull-stream')

//this is an in-memory index, must be rebuilt from the log.
module.exports = function (compare, decode) {
  if('function' !== typeof decode)
    throw new Error('decode is not function')
  if('function' !== typeof compare)
    throw new Error('compare is not function')
  //read the current max from the end of the start of the index file.

  var index = [], sorted = false
  var max = 0

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
    latest: function () { return max },
    get: get,
    add: function (data) {
      if(data.sync) return
      max = data.key
      sorted = false
      index.push({key: data.key, value: decode(data.value)})
    },
    search: function (target, cb) {
      sort()
      //we need to know the maximum value
      search(get, target, compare, 0, index.length, function (err, value, idx, exact) {
        cb(err, value, idx >= 0 ? index[idx].key : null, idx, exact)
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







