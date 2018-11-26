'use strict'
var Merge = require('pull-merge')
var pull = require('pull-stream/pull')
var Map = require('pull-stream/throughs/map')
var Stream = require('./stream')

module.exports = function (indexes, opts) {
  var keys = opts.keys === true
  var values = opts.values !== false
  opts.keys = true
  opts.values = true
  var compare = indexes[0].compare
  var reverse = (opts.reverse ? -1 : 1)
  return pull(
    Merge(indexes.map(function (i) {
        return i.stream(opts)
    }), function (a, b) {
      return compare(a.value, b.value) * reverse
    }),
    Map(function (e) {
      return keys && values ? e : keys ? e.key : e.value
    })
  )
}

