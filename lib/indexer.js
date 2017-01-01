/* jshint expr:true */

var async         = require('async'),
    _             = require('underscore'),
    EventEmitter  = require('events').EventEmitter,
    inherits      = require('util').inherits,
    Promise       = require('bluebird');

function Indexer () {
  EventEmitter.call(this);
}

var _defaultIndexer = function(item, options) {
  var index = {_type:options.type || item._type, _id: item._id};
  // XXX: by default avoid put index in the bulk request body if the target index was specified.
  if (!options.index) {
    index._index = item._index;
  }
  index = {index: index};
  if(options.parent !== '') {
    index.index._parent = item._source[options.parent];
  }

  return [
    index,
    item._source
  ];
};

inherits(Indexer, EventEmitter);

Indexer.prototype.index = function(docs, options, cb) {
  if (!docs) {
    return;
  }
  var self = this;
  options = options || {};
  options = options || {
    concurrency:require('os').cpus().length,
    bulk:100
  };
  options.indexer = options.indexer || _defaultIndexer;

  var chunks = _.toArray(_.groupBy(docs, function(item, index){ return Math.floor(index/options.bulk); }));
  async.eachLimit(chunks, options.concurrency, function(chunk, cb) {
    var bulk_data = [];
    chunk.forEach(function(item) {
      bulk_data = bulk_data.concat(options.indexer(item, options));
    });
    var bulkReq = {
      body: bulk_data
    };
    if (options.index) {
      bulkReq.index = options.index;
    }
    options.client.bulk(bulkReq, function(err, res) {
      if (err) {
        self.emit('error', err);
        return cb(err);
      }
      if (res.errors) {
        res.items.forEach(function(item) {
          if (_.indexOf([200, 201], item.index.status) == -1) {
            self.emit('item-failed', item);
          }
          self.emit('batch-complete', 1);
        });
      } else {
        self.emit('batch-complete', chunk.length);
      }
      cb(err);
    });
  }, function(err) {
    cb(err);
  });
};

Indexer.prototype.indexPromise = function(docs, options, cb) {
  if (!docs) {
    return;
  }

  var self = this;

  options = options || {};
  options = options || {
    concurrency:require('os').cpus().length,
    bulk:100,
  };

  options.indexer = options.indexer || _defaultIndexer;

  var chunks = _.toArray(_.groupBy(docs, function(item, index) {
    return Math.floor(index/options.bulk);
  }));

  async.eachLimit(chunks, options.concurrency, function(chunk, cb) {
    // holds whether the promise has finished
    var sent = false;

    // map each indexer to return a promise
    return Promise.all(chunk.map(function(item) {
      return options.indexer(item, options, options.client);
    })).catch(function (err) {
      self.emit('error', err);
      throw cb(sent = err);
    })

    // Once all of the data resolves index it
    .then(function (bulk_data) {
      bulk_data = _.flatten(bulk_data).filter(function(x) { return x; });

      if (!bulk_data.length) {
        return {};
      }

      var bulkReq = {
        body: bulk_data
      };
      if (options.index) {
        bulkReq.index = options.index;
      }
      return options.client.bulk(bulkReq);
    }).catch(function (err) {
      if (sent) throw err;

      self.emit('error', err);
      throw cb(sent = err);
    })

    // Indexed via ES successfully
    .then(function(res) {
      if (res.errors) {
        throw res;
      }

      self.emit('batch-complete', chunk.length);
      return cb();
    }).catch(function (err) {
      if (sent) throw err;

      err && Array.isArray(err.items) && err.items.forEach(function(item) {
        if (_.indexOf([200, 201], item.index.status) == -1) {
          self.emit('item-failed', item);
        }
        self.emit('batch-complete', 1);
      });

      cb(sent = err);
    });
  }, function(err) {
    cb(err);
  });
};

module.exports = Indexer;
