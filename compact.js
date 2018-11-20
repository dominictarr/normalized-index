'use strict'
var fs = require('fs')
var path = require('path')

var pull = require('pull-stream')
var Cat = require('pull-cat')
var WriteFile = require('pull-write-file')
var pCont = require('pull-cont')

var Index = require('./memory')
var FileTable = require('./file-table')
var MergeStream = require('./merge-stream')
var SparseMerge = require('./sparse')
var Group = require('pull-group')
var Obv = require('obv')
var mkdirp = require('mkdirp')
var cont = require('cont')
var cpara = require('cont').para

//generate an index following a log.

//needs improved compaction: needs a logarithimic compaction.
//maybe when hits a threashold, write to a file-table
//and a background process combines the oldest two equal sized
//tables.

//copy of the compaction strategy that just writes out files
//when they pass threashold. ends up with lots of small files.

//compaction strategy that holds two files, and compacts
//the new one into the big one. too slow, because recompacts
//too much.

function compaction (
  log, dir, compare, merge,
  indexes, index_to_compact, num_indexes, cb
) {
  var compacting = indexes.slice(index_to_compact, index_to_compact+num_indexes)
  var a = indexes[index_to_compact], b = indexes[index_to_compact+1]
  var latest = compacting[0].latest()
  //compacting a single index only makes sense the first time the memtable is written

//  if(compacting.length === 1)
//    return cb(null, {count:0, total:0, average: NaN, since: latest})

  var filename = path.join(dir, ''+Date.now()+'.idx')
  var c = 0, t = 0
  pull(
    Cat([
      //save the `latest` because the file format keeps the length
      //as the first value.
      pull.values([[latest]]),
      compacting.length == 2
      ? merge(compacting[0], compacting[1], compare)
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
      var i = indexes.indexOf(compacting[0])
      var new_index = FileTable(
//        path.join(dir, compacting[0].latest() +'.idx'),
//        path.join(dir, compacting[0].latest() +'.idx'),
        filename,
        log,
        compare
      )
      _indexes.splice(
        //search for the index again, just incase another compaction has happened.
        i,
        compacting.length,
        //and insert the new index.
        new_index
      )
      new_index.ready(function () {
        cb(null, _indexes.filter(Boolean), {
          input: compacting.map(function (index) {
            return {size: index.length(), latest: index.latest()}
          }),
          count: c, total: t,
          average: t/c, since: latest
        })
      })
    })
  )

}

function compact2 (log, dir, compare, indexes, cb) {
  return compaction(log, dir, compare, SparseMerge, indexes, 0, 2, cb)
}

function compact_single (log, dir, compare, indexes, cb) {
  return compaction(log, dir, compare, null, indexes, 0, 1, cb)
}

function compact_recursive (log, dir, compare, indexes, cb) {
  var data = {count:0, total:0, average:0,  since:0}
  if(indexes.length === 1)
    return compaction(log, dir, compare, null, indexes, 0, 1, cb)
  if(indexes[0].length() < indexes[1].length()) {
    return compaction(log, dir, compare, null, indexes, 0, 1, cb)
  }

  //if we have two indexes, approx the same size
  ;(function recurse (i) {
    if(i+1 >= indexes.length) return cb(null, indexes, data)
    if(indexes[i].length() < indexes[i+1].length())
      return cb(null, indexes, data)

    console.error("COMPACTING", i, i+1, indexes[i].length(), indexes[i+1].length())
    compaction(log, dir, compare, SparseMerge, indexes, i, 2, function (err, _indexes, _data) {
      if(err) return cb(err)

      indexes = _indexes
      data.count += _data.count
      data.total += data.total
      data.average = data.count/data.total
      data.since = Math.max(data.since, _data.since)
      data.sizes = indexes.map(function (e) {
        return {latest: e.latest(), length: e.length()}
      })
      console.log(data.sizes)

      recurse(i+1)
    })
  })(0)

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
//        cpara(indexes.map(function (e) {
//          return function (cb) { e.ready ? e.ready(cb) : cb() }
//        })) (function () {
//          since.set(meta.since)
//        })

//        log.since.once(function (_v) {
//          var start = Date.now()
//          since(function (v) {
//            if(v === _v) console.log('loaded', Date.now()-start)
//          })
//        })

      }
    })
  })

  return {
    since: since,
    compact: function (_cb) {
      var _meta

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

      var latest = indexes[0].latest()
      indexes.unshift(Index(compare))
      compact_recursive(log, dir, compare, indexes.slice(1), function (err, _indexes, status) {
        if(err) return cb(err)
        fs.writeFile(metafile+'~', JSON.stringify(_meta = {
          since: latest, index: _indexes.map(function (e) {
            return path.basename(e.filename)
          }).filter(Boolean)
        }), function (err) {
          if(err) return cb(err)
          fs.rename(metafile+'~', metafile, function (err) {
            if(err) return cb(err)
            indexes = [indexes[0]].concat(_indexes)
            status.meta = meta = _meta
            cb(err, status)
          })
        })
      })


    },
    stream: function (opts) {
      opts = opts || {}
      if(opts && opts.index != null)
        return indexes[opts.index].stream(opts)
      return MergeStream(indexes, opts)
    },
    add: function (op) {
      //only add to most recent index
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

