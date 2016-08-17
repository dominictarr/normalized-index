var search = require('binary-search-async')
var pull = require('pull-stream')

module.exports = function (dir, log, compare, decode) {

  //read the current max from the end of the start of the index file.

  var index = [], sorted = false
  var max = 0
  pull(
    log.stream({live: true, sync: false, keys: true}),
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
    log.get(index[i].key, function (err, value) {
      cb(null, decode(value))
    })
  }

  return {
    get: get,

    search: function (target, cb) {
      sort()
      //we need to know the maximum value
      search(get, target, compare, 0, index.length, function (err, value, idx) {
        cb(err, index[idx].key, value)
      })
    }
  }
}
















