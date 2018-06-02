var fs = require('fs')
var path = require('path')

var pull = require('pull-stream')
var Cat = require('pull-cat')
var WriteFile = require('pull-write-file')
var pCont = require('pull-cont')

var Index = require('./index')
var FileTable = require('./file-table')
var Stream = require('./stream')
var SparseMerge = require('./sparse-merge')
var Group = require('pull-group')
//generate an index following a log.

// a simple index with two stages.
// one is immutable, and read from disk
// the other is in memory.

// when compact(cb) is called, these two indexes are combined into one.

module.exports = function (log, dir, compare) {

  var metafile = path.join(dir, '/meta.json')
  var indexes = [Index(compare)]
  var cbs = []

  return {
    compact: function (_cb) {
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

      var compacting = indexes.slice(0, 2)
      indexes.unshift(Index(compare))
      var filename = path.join(dir, ''+Date.now()+'.idx')
      var latest = compacting[0].latest()
      var c = 0, t = 0
      pull(
        Cat([
          //save the `latest` because the file format keeps the length
          //as the first value.
          pull.values([[latest]]),
          compacting.length == 2
          ? SparseMerge(compacting[0], compacting[1])
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
          fs.writeFile(metafile+'~', JSON.stringify(_meta = {
            since: latest, index: path.relative(dir, filename)
          }), function (err) {
            if(err) return cb(err)
            fs.rename(metafile+'~', metafile, function (err) {
              if(err) return cb(err)
              indexes.splice(
                indexes.indexOf(compacting[0]),
                compacting.length,
                FileTable(filename, log, compare)
              )
              cb(err, {count: c, total: t, average: t/c, since: _meta.since})
            })
          })
        })
      )
    },
    stream: function (opts) {
      if(indexes.length == 2)
        return pCont(function (cb) {
          indexes[1].ready(function () {
            cb(null, Stream(indexes, opts, compare))
          })
        })
      return Stream(indexes[0], opts, compare)
    },
    add: function (op) {
      //only add to most recent index
      indexes[0].add(op)
    },
    status: function () {
      return {
        compacting: !!cbs.length,
        indexes: indexes.length,
        meta: meta
      }
    },
    indexes: indexes
  }
}




