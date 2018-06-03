var rewind = require('./reverse-search')
/*
  algorithm to binary search that can also find the next item
  greater than a given thing cheaply.
*/
module.exports = function (get, compare, low, high) {
  var mid, lo = low, hi = high, _target
  //standard binary search for target.
  function search (target, cb) {
  console.log('search', target)
    mid = lo + (hi - lo >> 1)
    get(mid, function (err, value) {
      if(err) return cb(err)
      var c = compare(value, target)
      if(c < 0) hi = mid - 1
      else if (c > 0) lo = mid + 1
      else return cb(null, value, mid) //found

      if(lo > hi) cb(null, value, ~mid) //found next best thing
      else search(target, cb)
    })
  }

  function seek (target, cb) {
    console.log('seek:', target)
    var prev = rewind(mid, low, high)
    console.log('prev', prev)
    get(prev.hi, function (err, value) {
      var c = compare(value, target)
      if(c > 0) { //if value is still lower than target, kep looking.
        mid = prev.hi;
        seek(target, cb)
      }
      else if(c < 0) {
        hi = prev.hi; lo = prev.lo
        search(target, cb)
      }
      else if(c == 0)
        cb(null, value, prev.hi)
    })
  }

  return function (target, cb) {
    if(!_target) { //this is the first seek, so just do binary search.
      search(_target = target, cb)
    }
    else {
      if(compare(_target, target) <= 0) throw new Error('targets must be increasing')
      seek(target, cb)
    }
  }
}



