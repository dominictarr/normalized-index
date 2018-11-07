var TestFlumeViewIndex = require('test-flumeview-index/bench')

var Flume = require('flumedb')
var Log = require('flumelog-offset')
var Index = require('../')
//var codec = require('flumecodec/json')

var decodes = 0, time = 0, start = Date.now()
var codec = {
  encode: function (o) {
    var s = JSON.stringify(o)
    return s
  },
  decode: function (s) {
    decodes ++
    var start = process.hrtime()
    var v = JSON.parse(s.toString())
    time += process.hrtime(start)[1]
    return v
  },
  buffer: false,
}


require('test-flumeview-index/bench')(function (file, seed) {
  return Flume(Log(file+'/log.offset', {blockSize:64*1024, codec:codec}))
    .use('index', Index(1, [['key']]))
}, 65e3)



process.on('exit', function () {
  var seconds_decoding = time/1000000000
  var seconds = ((Date.now()-start)/1000)
  console.error('decodes', decodes, seconds, seconds_decoding, seconds_decoding / seconds )
  console.error('memory', process.memoryUsage())
})

