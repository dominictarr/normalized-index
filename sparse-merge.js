'use strict'
/*
  merge two indexes, but don't read every value.
  take the next value from a, then search for that value in b.
  now we know that everything before that index in b is before
  that value, so return that. switch a and b.
  the next read searches for the next greater value in what was a.
  and returns that range.

  the resulting stream is written to a new index.
  if one index is larger than the other, less comparsons are used.
  or if the data is already kinda sorted, so that each side of
  the merge only overlaps a bit. If there is no overlap, only one
  comparison will be necessary.

  on a test merge with 900 vs 100 items, just 184 individual ranges
  were returned.

  TODO: currently, searching always starts from the top, even though
  we know the bits at the start are smaller. however, this means
  the cache works well, because the first few queries will be in the
  cache. It would be even better to skip these comparsons, while
  maintaining alignment though.

  XXX: this code _looks elegant_ but it's slower than streaming
  the whole thing. seems like too many seaches. so should solve
  that above problem. on a uniformly random ordering ranges are
  too small for this method to help much.
*/

module.exports = function (a, b) {
  var i = 0, j = 0
  return function read (abort, cb) {
    if(i >= a.length()) {
      if(j < b.length()) {
        b.range(j, b.length()-1, function (err, range) {
          j = b.length() //so next read will end
          if(range.length) cb(err, range)
          else cb(true)
        })
      }
      else
        return cb(true)
    }
    else
      a.get(i, function (err, value) {
        b.search(value, function (err, _, __, _j) {
          if(_j < 0) _j = ~_j
          b.range(j, _j-1, function (err, range) {
            var tmp = b; b = a; a = tmp; j = i; i = _j
            setImmediate(function () {
              if(range.length) cb(err, range)
              else read(null, cb)
            })
          })
        })
      })
    }
}




