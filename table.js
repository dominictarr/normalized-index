var search = require('binary-search-async')
module.exports = function (table, log, compare, decode) {
  function offset(index) {
    return table.readUInt32BE(4+index*4)
  }
  var max = table.length/4 - 1

  function get (i, cb) {
    console.log('get', i, max)
    setImmediate(function () {
      log.get(offset(i), function (err, value) {
        value = decode(value)
        console.log(value.key)
        cb(null, value)
      })
    })
  }
  return {
    get: get,
    length: function () { return max },
    search: function (target, cb) {
      console.log('search:', target)
      //we need to know the maximum value
      search(get, target, compare, 0, max, function (err, value, idx) {
        cb(err, value, offset(idx), idx)
      })
    }
  }

}


