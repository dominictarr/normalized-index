'use strict'
var search = require('binary-search-async')
var pull = require('pull-stream')
var seek = require('binary-search-async/seek')
var Stream = require('./stream')

//this is an in-memory index, must be rebuilt from the log.

//leveldb uses a skiplist for this. but to get things working
//i just used an array + sorting it when adding something.
//much faster than binary search and then insert (since splice is slow)
//and a skip list is hard to implement (like linked list but worse!)

global.SORT_COUNT = 0
global.SORT_TIME = 0
global.SORT_MAX = 0
module.exports = function (compare) {
  if('function' !== typeof compare)
    throw new Error('compare is not function')
  //read the current max from the end of the start of the index file.

  var index = [], sorted = false
  var max = -1

  function sort () {
    if(!sorted) {
      var start = Date.now()
      index.sort(function cmp (a, b) {
        return compare(a.value, b.value) || a.key - b.key
      })
      var time = Date.now()-start
      global.SORT_COUNT += 1
      global.SORT_TIME+=time
      global.SORT_MAX = Math.max(global.SORT_MAX, time)
      console.log('sort time:', time, global.SORT_TIME, global.SORT_MAX, index.length)
      sorted = true
    }
  }

  function get (i, cb) {
    sort()
    if(i < 0 || i >= index.length)
      cb(new RangeError('out of bounds:'+i))
    else {
      cb(null, index[i].value, index[i].key, i)
    }
  }
  var self
  return self = {
    length: function () { return index.length },
    latest: function () { return max },
    compare: compare,
    get: get,
    range: function (start, end, cb) {
      sort()
      var a = []
      for(var i = start; i <= end; i++) {
        if(i >= index.length) throw new Error('index greater than index.length:'+i)
        a.push(index[i].key)
      }
      cb(null, a)
    },
    add: function (data) {
      if(data.sync) return
      max = data.key
      sorted = false
      index.push(data)
    },
    seek: function (target, start, cb) {
      sort(),
      seek(get, target, compare, start, 0, self.length()-1, cb)
    },
    search: function (target, cb) {
      if(self.length() === 0) cb(new Error('not found'))
      sort()
      //we need to know the maximum value
      search(get, target, compare, 0, self.length()-1, function (err, idx, value) {
        cb(err, value, idx >= 0 ? index[idx].key : null, idx, idx>=0)
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
    },
    stream: function (opts) {
      //TODO: stream should hold a reference to the index.
      //and index should be copied if sort() is called while streaming.
      //(or something like that to create snapshots)
      return Stream(this, opts)
    }
  }
}

process.on('exit', function () {
  console.log("normalized-index:sorts", SORT_COUNT, SORT_TIME, SORT_MAX)
})


