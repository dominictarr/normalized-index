
module.exports = function (target_mid, lo, hi) {
  var _lo = lo, _hi = hi
  while(lo <= hi) {
    var mid = lo + (hi - lo >> 1)
    if(hi > target_mid) _hi = hi
    if(lo < target_mid) _lo = lo
    if(target_mid < mid) hi = mid - 1
    else if(target_mid > mid) lo = mid + 1
    else return {hi:_hi, lo:_lo}
  }
}

console.log(
  module.exports.apply(null, process.argv.slice(2).map(Number))
)






