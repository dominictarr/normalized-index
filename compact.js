var Index = require('./index')
var FileTable = require('./file-table')
var pull = require('pull-stream')
var fs = require('fs')
var Cat = require('pull-cat')
var Stream = require('./stream')
var Group = require('pull-group')
var WriteFile = require('pull-write-file')
//generate an index following a log.

// okay, a simple index with two stages.
// one is immutable, and read from disk
// the other is in memory.

// once the size of the index passes a threashold,
// merge it with the old one.

module.exports = function (log, dir, compare, decode) {

  var metafile = dir+'/meta.json'
  var indexes = [Index(compare, decode)]
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
          cbs.push(function (err) {
            if(err) _cb(err)
            else self.compact(_cb) //start another compaction
          })
      }

      cbs = [_cb]

      function cb (err, meta) {
        var _cbs = cbs
        cbs = []
        while(_cbs.length) _cbs.shift()(err, meta)
      }

      var compacting = indexes.slice()
      indexes.unshift(Index(compare, decode))
      var filename = dir+'/'+Date.now()
      var latest = compacting[0].latest()
      pull(
        Cat([
          pull.values([latest]),
          Stream(compacting, {keys: true, values: false}, compare)
        ]),
        //TODO: rewrite without buffering.
        Group(256),
        pull.map(function (ary) {
          var buf = new Buffer(ary.length*4)
          for(var i = 0; i < ary.length; i++)
            buf.writeUInt32BE(ary[i], i*4)
          return buf
        }),
        WriteFile(filename, function (err) {
          if(err) return cb(err)

          //write metafile to temp location, then move it into place,
          //so that compaction is atomic.
          fs.writeFile(metafile+'~', JSON.stringify(_meta = {
            since: latest, index: filename
          }), function (err) {
            if(err) return cb(err)
            fs.rename(metafile+'~', metafile, function (err) {
              if(err) return cb(err)
              indexes.splice(
                indexes.indexOf(compacting[0]),
                compacting.length,
                FileTable(filename, log, compare, decode)
              )
              cb(err, meta = _meta)
            })
          })
        })
      )
    },
    stream: function (opts) {
      return Stream(indexes, opts, compare)
    },
    add: function (op) {
      //only add to most recent index
      indexes[0].add(op)
    }
  }
}

