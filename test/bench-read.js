var fs = require('fs')
var path = require('path')
var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var path = require('path')
var FileTable = require('../file-table')

//var rmrf = require('rimraf')
//var mkdirp = require('mkdirp')
//var Compact = require('../compact')
//var SparseMerge = require('../sparse-merge')
//var Stream = require('../stream')
//
function compare (a, b) {
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
}
var pull = require('pull-stream')
var Stream = require('../stream')

module.exports = function (dir) {

  var log = FlumeLog(path.join(dir, 'log'), {codec: json})

  var meta = JSON.parse(fs.readFileSync(path.join(dir, 'meta.json')))
  console.log(meta)
  var ft = FileTable(path.join(dir, meta.index[0]), log, compare)

  var N = 1
  var i = N, a = []
    var start = Date.now()
  ;(function next () {
    if(!i--) return       console.log((Date.now()-start)/N)
      pull(
        Stream(ft, {gt:{key:Math.random()}}),
        pull.take(100),
        pull.drain(function (data) { a.push(data) }, function (err, value) {
          console.log((Date.now()-start)/a.length, a.length)
          console.log(err, value)
          next()
        })
      )
    //})
  })()
}

module.exports('/tmp/test-normalized-index_compact')


