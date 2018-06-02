var fs = require('fs')
var tape = require('tape')
var pull = require('pull-stream')
var path = require('path')
var Flume = require('flumedb')
var FlumeLog = require('flumelog-offset')
var FlumeViewLevel = require('flumeview-level')
var json = require('flumecodec/json')
var Index = require('../memory')
var IndexTable = require('../table')
var FileTable = require('../file-table')
var mkdirp = require('mkdirp')
var rmrf = require('rimraf')

var dir = '/tmp/test-normalized-index'

var N = 10000

var string_codec = {
  decode: function (b) { return b.toString('ascii') }
}

var log = FlumeLog(path.join(dir, 'offset.log'), {codec: json})
var db = Flume(log).use('level', FlumeViewLevel(1, function (value) {
  return [value.hash]
}))

function pluck (raw) {
//  console.log(raw, /([a-zA-Z0-9\+\/]+\=)/.exec(raw))
  return /([a-zA-Z0-9\+\/]+=)/.exec(raw)[1]
}

function _compare (a, b) {
  var h = pluck(a)
//  console.log('compare', h)
  return h < b.hash ? -1 : h > b.hash ? 1 : 0
}

function compare (a, b) {
  return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0
}

var crypto = require('crypto')

function hash (s) {
  return crypto.createHash('sha256').update(s.toString()).digest('base64')
}

var table = FileTable(path.join(dir, 'table'), log, compare)

var target = {hash: hash(+process.argv[2])}
var target2 = {hash: hash(+process.argv[3])}

console.log('targets', target.hash, target2.hash)
var level = process.argv[2] == 'level'

var targets = process.argv.slice(2).filter(Number).map(hash)

;(function next () {
  var start = Date.now()
//  db.level.get(target.hash, function (err, value) {
  //  console.log('level', Date.now()-start, value)
    if(!targets.length) return
    
    start = Date.now()
    var query = {hash: targets.shift()}

    if(level) db.level.get(query.hash, cb)
    else      table.search(query, cb)

    function cb (err, value, offset) {
      console.log(level ? 'level' : 'ni', Date.now()-start, value, offset)
      next()
    }
})()

