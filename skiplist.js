

module.exports = function (path) {

  return {
    length: function () { return last },
    latest: function () { return max },
    get: function () {
      throw new Error('skiplist index does not provide get')
    },
  }

}


