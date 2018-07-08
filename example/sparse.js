var pull = require('pull-stream')
var sparse = require('../sparse')

function compare (a, b) { return a - b }
var k = 0
function create (name, N, initial, mult) {
  var ary = [initial]
  for(var i = 0; i < N; i++)
    ary.push(Math.random()*(mult | 0))
  ary.sort(compare)
//  console.log(name, ary)
  return {
    array: ary,
    get: function (i, cb) {
      k++
      return cb(null, ary[i])
    },
    range: function (start, end, cb) {
      cb(null, ary.slice(start, end+1))
    },
    length: function () { return ary.length }
  }
}

var N = 1000
var a = create('A', N, 1, 50)
var b = create('B', N, 0, 25)
pull(
  sparse(a, b,compare),
  pull.collect(function (err, ary) {
    var cat = [].concat.apply([], ary)
    console.log(cat.length, k)
  })
)
k = 0
pull(
  require('../sparse-merge')(a, b, compare),
  pull.collect(function (err, ary) {
    var cat = [].concat.apply([], ary)
    console.log(cat.length, k)
  })
)






