'use strict'
var CompareAt = require('compare-at')
var Compactor = require('./compact')
var path = require('path')
var Take = require('pull-stream/throughs/take')
var K = 65536/2

module.exports = function FlumeViewNormalizedIndex (version, paths) {

  var compare = CompareAt(paths), has = CompareAt.hasPath(paths)

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
          if(err) return cb(err === true ? null : err)
          if(has(data.value)) {
              compactor.add({key:data.seq, value:data.value})
            //simple strategy for managing write flow:
            //when we hit a threashold, wait until we have compacted.
            //It would be better to dump tables quickly,
            //then compact them in the background.
            if(compactor.indexes()[0].length() < K)
              read(null, again)
            else {
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
            setImmediate(function () {
              compactor.since.set(data.seq)
              read(null, again)
            })
          }
        })
      }
    }

    compactor.get = function (key, cb) {
      compactor.search({key:key}, cb)
//      var source = compactor.stream({gte: {key:key}, limit: 1})
//      source(null, function (err, data) {
//        if(err === true) cb(new Error('not found'))
//        else if(err) cb(err)
//        else source(true, function () {
//          cb(null, data)
//        })
//      })
    }

    compactor.search = function (target, cb) {
      var _indexes = compactor.indexes() //take a snapshot, incase of a compaction
      ;(function recurse (i) {
        if(i >= _indexes.length) cb(new Error('not found'))
        else
          _indexes[i].search(target, function (err, value, seq) {
            if(compare(target, value) === 0) cb(null, value)
            else recurse(i+1)
          })
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

