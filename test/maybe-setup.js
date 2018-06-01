

try {
  require('fs').statSync('/tmp/test-normalized-index')
} catch (err) {
  require('./scripts/setup')
}

