'use strict'
var tape = require('tape')
var NormalizedIndex = require('./')
var pull = require('pull-stream')
var path = require('path')
var Flume = require('flumedb')
var FlumeLog = require('flumelog-offset')
var json = require('flumecodec/json')
var mkdirp = require('mkdirp')
var rmrf = require('rimraf')

var CompareAt = require('compare-at')

var crypto = require('crypto')

function hash (s) {
  return crypto.createHash('sha256').update(String(s)).digest('base64')
}

var dir = path.join(process.env.HOME, '.ssb/flume')
console.log(dir)
var string_codec = {
  decode: function (b) { return b.toString('ascii') }
}

var log = FlumeLog(path.join(dir, 'log.offset'), {codec: json})
var db = Flume(log)

var indexes = [
  {key: 'log', value: [['timestamp']]},
  {key: 'clk', value: [['value', 'author'], ['value', 'sequence']] },
  {key: 'typ', value: [['value', 'content', 'type'], ['timestamp']] },
  {key: 'tya', value: [['value', 'content', 'type'], ['value', 'timestamp']] },
  {key: 'cha', value: [['value', 'content', 'channel'], ['timestamp']] },
  {key: 'aty', value: [['value', 'author'], ['value', 'content', 'type'], ['timestamp']]},
  {key: 'ata', value: [['value', 'author'], ['value', 'content', 'type'], ['value', 'timestamp']]},
]

indexes.forEach(function (opts) {
  db.use('ni_'+opts.key, NormalizedIndex(1, opts.value))
})


var compare = CompareAt([['value','content','type'], ['value','timestamp']])

var disorder = 0, limit = 1000
;(function more (ts) {
  var start = Date.now(), min = ts, c = 0, last = null
  var orders = {'-1': 0, '0': 0, '1': 0}

  pull(
    db.ni_tya.read({
      gt: {
        value: {
          timestamp: min,
          content: {type: 'contact'}
        },
      },
      lte: {
        value: {
          timestamp: +new Date('2018-01-01'),
          content: {type: 'contact'}
        },
      },
      limit: limit
    }),
    pull.through(function (next) {
      var cmp = compare(last, next)
      orders[cmp] = (orders[cmp] || 0) + 1
      if(last && cmp >= 0) {
        console.log([
          last.value.content.type, next.value.content.type
        ],
          next.value.timestamp - last.value.timestamp
        )
        disorder ++
      }
      last = next
    }),
    pull.drain(function (data) {
      c++
      min = Math.max(min, data.value.timestamp)
      //console.log(data.value.content.type, new Date(data.value.timestamp), c)
    }, function () {
      var time = (Date.now() - start)/1000
      console.log('READ', {count: c, time: time, disordered: disorder}, orders)

      if(c == limit) more(min)
    })
  )
})(0)

