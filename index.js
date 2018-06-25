'use strict'
var CompareAt = require('compare-at')
var Compactor = require('./compact')
var path = require('path')
var Take = require('pull-stream/throughs/take')
var K = 50*1000

module.exports = function FlumeViewNormalizedIndex (version, index) {

  var compare = CompareAt(index), has = CompareAt.hasPath(index)

  return function (log, name) {
    if(!log.filename) throw new Error('in memory index not supported')
    var compactor = Compactor(
      log,
      path.join(path.dirname(log.filename), name),
      compare
    )

    if(!compactor) throw new Error('weird')
    compactor.createSink = function (cb) {
      return function (read) {
        read(null, function again (err, data) {
          if(has(data.value)) {
              compactor.add({key:data.seq, value:data.value})
            //simple strategy for managing write flow:
            //when we hit a threashold, wait until we have compacted.
            if(compactor.indexes[0].length() < K)
              read(null, again)
            else {
              console.log(name+':compacting', compactor.indexes.map(function (e) { return e.length() }))
              var start = Date.now(), c = compactor.indexes.reduce(function (a, b) {
                return a + b.length() || 0
              }, 0)
              compactor.compact(function (err, status) {
                console.log(name+':compacted!', Date.now() - start, c, status)
                read(null, again)
              })
            }
          }
          else {
            setImmediate(function () {
              compactor.since.set(data.seq)
              read(null, again)
            })
          }
        })
      }
    }

    compactor.read = function (opts) {
      if(opts && opts.limit != null)
        return Take(opts.limit)(compactor.stream(opts))
      return compactor.stream(opts)
    }

    compactor.methods = {
      compact: 'async',
      status: 'sync',
      read: 'source'
    }
    return compactor
  }
}



