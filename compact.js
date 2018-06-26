'use strict'
var fs = require('fs')
var path = require('path')

var pull = require('pull-stream')
var Cat = require('pull-cat')
var WriteFile = require('pull-write-file')
var pCont = require('pull-cont')

var Index = require('./memory')
var FileTable = require('./file-table')
var Stream = require('./stream')
var SparseMerge = require('./sparse')
var Group = require('pull-group')
var Obv = require('obv')
var mkdirp = require('mkdirp')

//generate an index following a log.

// a simple index with two stages.
// one is immutable, and read from disk
// the other is in memory.

// when compact(cb) is called, these two indexes are combined into one.

global.TS = Date.now()
global.C = 0

function compact (log, dir, compare, indexes, cb) {
  var compacting = indexes.slice(0, 2)
  indexes.unshift(Index(compare))
  var latest = compacting[0].latest()
  var filename = path.join(dir, ''+latest+'.idx')
  var c = 0, t = 0
  pull(
    Cat([
      //save the `latest` because the file format keeps the length
      //as the first value.
      pull.values([[latest]]),
      compacting.length == 2
      ? SparseMerge(compacting[0], compacting[1], compare)
      : pCont(function (cb) {
          compacting[0].range(0, compacting[0].length()-1, function (err, range) {
            cb(null, pull.values([range]))
          })
        })
    ]),
    //TODO: rewrite without buffering.
    pull.map(function (ary) {
      c ++; t += Buffer.isBuffer(ary) ? ary.length/4 : ary.length 
      if(Buffer.isBuffer(ary)) return ary
      var buf = new Buffer(ary.length*4)
      for(var i = 0; i < ary.length; i++)
        buf.writeUInt32BE(ary[i], i*4)
      return buf
    }),
    Group(1024*16),
    pull.map(function (ary) {
      return Buffer.concat(ary)
    }),
    WriteFile(filename, function (err) {
      if(err) return cb(err)
      //write metafile to temp location, then move it into place,
      //so that compaction is atomic.
      var _indexes = indexes.slice()
      _indexes.splice(
        indexes.indexOf(compacting[0]),
        compacting.length,
        FileTable(
          path.join(dir, compacting[0].latest() +'.idx'),
          log,
          compact
        )
      )
      cb(null, _indexes.filter(Boolean), {
        count: c, total: t,
        average: t/c, since: latest
      })
    })
  )
}

module.exports = function (log, dir, compare) {
  var metafile = path.join(dir, '/meta.json')
  var indexes = [Index(compare)]
  var cbs = [], meta
  var since = Obv()
  mkdirp(dir, function () {
    fs.readFile(metafile, function (err, value) {
      if(err)
        since.set(-1)
      else {
        try {
          meta = JSON.parse(value)
          if(!Array.isArray(meta.index)) //old format
            meta.index = [meta.index]
        } catch (err) {
          return since.set(-1)
        }
        meta.index.forEach(function (index) {
          indexes.push(FileTable(
            path.join(dir, index),
            log,
            compare
          ))
        })
        since.set(meta.since)
      }
    })
  })

  return {
    since: since,
    compact: function (_cb) {
      var _meta
      //if already compacting,
      //wait until compaction is complete,
      //then compact again.

      //to compact, create a new index,

      if(cbs.length) {
        //if a compaction is underway,
        //and there have not been anymore messages added,
        //we only need one compaction.
        if(!indexes[0].length())
          return cbs.push(_cb)
        else
          cbs.push(function (err, status) {
            if(err) _cb(err, status)
            else self.compact(_cb) //start another compaction
          })
      }

      cbs = [_cb]

      function cb (err, meta) {
        var _cbs = cbs
        cbs = []
        while(_cbs.length) _cbs.shift()(err, meta)
      }

      compact(log, dir, compare, indexes, function (err, _indexes, status) {
        if(err) return cb(err)
        fs.writeFile(metafile+'~', JSON.stringify(_meta = {
          since: indexes[0].latest(), index: _indexes.map(function (e) {
            return e.filename
          }).filter(Boolean)
        }), function (err) {
          if(err) return cb(err)
          fs.rename(metafile+'~', metafile, function (err) {
            if(err) return cb(err)
            indexes = _indexes
            status.meta = meta
            cb(err, status)
          })
        })
      })


    },
    stream: function (opts) {
      //TODO: I'm always fixing this thing in stream where
      //it doesn't compare on the value property.
      function _compare (a, b) {
        return compare(a.value, b.value)
      }

      if(opts && opts.index != null)
        return Stream(indexes[opts.index], opts, _compare)

      if(indexes.length == 2)
        return pCont(function (cb) {
          indexes[1].ready(function () {
            cb(null, Stream(indexes[0].length() ? indexes : indexes[1], opts, _compare))
          })
        })
      return Stream(indexes[0], opts, _compare)
    },
    add: function (op) {
      //only add to most recent index
      C++
      if(Date.now() > TS+1000) {
        TS = Date.now()
        console.log('UPDATE', op.key, C)
        C = 0
      }

      indexes[0].add(op)
      since.set(op.key)
    },
    status: function () {
      return {
        compacting: !!cbs.length,
        indexes: indexes.length,
        since: indexes.map(function (e) {
          return e.latest()
        }),
        lengths: indexes.map(function (e) {
          return e.length()
        }),
        meta: meta
      }
    },
    indexes: function () { return indexes }
  }
}

