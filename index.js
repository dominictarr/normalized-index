'use strict'
var CompareAt = require('compare-at')
var Compactor = require('./compact')
var path = require('path')
var Take = require('pull-stream/throughs/take')
var K = 65536

module.exports = function FlumeViewNormalizedIndex (version, opts) {
  if(Array.isArray(opts)) {
    opts = {paths: opts}
  }
  var compare = opts.compare || CompareAt.createCompareAuto(opts.paths)
  var has = opts.has || CompareAt.hasPath(opts.paths)

  return function (log, name) {
    if(!log.filename) throw new Error('in memory index not supported')
    var compactor = Compactor(
      log,
      path.join(path.dirname(log.filename), name),
      compare
    )

    if(!compactor) throw new Error('weird')
    var C = 0
    compactor.createSink = function (cb) {
      return function (read) {
        read(null, function again (err, data) {
          if(err) return cb(err === true ? null : err)
          if(has(data.value)) {
              compactor.add({key:data.seq, value:data.value})
            //simple strategy for managing write flow:
            //when we hit a threashold, then compact.
            //when we catch up with main log, compact.
            //reading stops during compaction.

            function nice (paths) {
              return paths.map(function (e) {
                return e.join('.')
              }).join(';')
            }

            if(compactor.indexes()[0].length() < K && (log.since.value === undefined || data.seq < log.since.value)) {
              //set immediate here makes building many indexes
              //significantly faster, because it caches much better.
              //without setImmediate, one index races ahead,
              //while the others are still behind, so the first
              //reads many values into the cache, pushes the old
              //values out, and they must then read them again.
              //difference is big:
              // 9 indexes in 9 minutes without this
              // but only takes 6 minutes with this!
              //(flumelog-query does the same 9 indexes in 3 min, beating that is the target)
              setImmediate(function () { read(null, again) })
            } else {
              console.error(name+':compacting', compactor.indexes().map(function (e) { return e.length() }))
              var start = Date.now(), c = compactor.indexes().reduce(function (a, b) {
                return a + b.length() || 0
              }, 0)
              compactor.compact(function (err, status) {
                console.error(name+':compacted!', Date.now() - start, c)
                console.error(status)
                read(null, again)
              })
            }
          }
          else {
            //compact when the index gets in sync with the log.
            //not every record is in every index, so maybe
            //compact even if we did not add anything.
            compactor.since.set(data.seq)
            if(data.seq === log.since.value)
              compactor.compact(function (err, status) {
                console.error(name+':compacted!', Date.now() - start, c)
                console.error(status)
                read(null, again)
              })
            else
              setImmediate(function () {
//                compactor.since.set(data.seq)
                read(null, again)
              })
          }
        })
      }
    }

    compactor.get = function (values, cb) {
      compactor.search('string' === typeof values ? [values] : values, cb)
    }

    compactor.search = function (target, cb) {
      var _indexes = compactor.indexes() //take a snapshot, incase of a compaction
      ;(function recurse (i) {
        if(i >= _indexes.length) cb(new Error('not found'))
        else if(_indexes[i].length())
          _indexes[i].search(target, function (err, value, seq) {
            if(compare(value, target) === 0) cb(null, value)
            else recurse(i+1)
          })
        else
          recurse(i+1)
      })(0)
    }

    compactor.read = function (opts) {
      if(opts && opts.reverse) {
        return function (_, cb) { cb(new Error('not supported')) }
      }
      if(opts && opts.limit != null)
        return Take(opts.limit)(compactor.stream(opts))
      return compactor.stream(opts)
    }

    compactor.methods = {
      compact: 'async',
      status: 'sync',
      read: 'source',
      indexes: 'sync',
      get: 'async'
    }
    return compactor
  }
}
