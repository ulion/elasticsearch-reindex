#!/usr/bin/env node

var cli           = require('commander'),
    elasticsearch = require('elasticsearch')
    async         = require('async'),
    cluster       = require('cluster'),
    _             = require('underscore'),
    bunyan        = require('bunyan'),
    ProgressBar   = require('progress'),
    fs            = require('fs'),
    Indexer       = require('../lib/indexer'),
    URI           = require('URIjs');


cli
.version('1.0.12')
.option('-f, --from [value]', 'source index, eg. http://192.168.1.100:9200/old_index/old_type')
.option('-t, --to [value]', 'to index, eg. http://192.168.1.100:9200/new_index/new_type')
.option('-c, --concurrency [value]', 'concurrency for reindex', require('os').cpus().length)
.option('-b, --bulk [value]', 'bulk size for a thread', 100)
.option('-s, --scroll [value]', 'default 1m', '1m')
.option('-o, --request_timeout [value]', 'default 60000', 60000)
.option('-l, --log_path [value]', 'default ./reindex.log', './reindex.log')
.option('-n, --max_docs [value]', 'default -1 unlimited', -1)
.parse(process.argv);

var logger        = bunyan.createLogger({
  src: true,
  name: "elasticsearch-reindex",
  streams: [{
    path: cli.log_path
  }]
});


var custom_indexer = cli.args[0] ? require(fs.realpathSync(cli.args[0])) : null;

if (cluster.isMaster) {
  if (custom_indexer.sharded) {
    custom_indexer.sharded.ranges.forEach(function(shard) {
      var worker_arg = {range:{}, name: shard.name};
      worker_arg.range[custom_indexer.sharded.field] = shard.range;
      cluster.fork({worker_arg:JSON.stringify(worker_arg)});
    });
  } else {
    cluster.fork();
  }
} else {
  var range = null;
  var shard_name = '';
  if (true) {}
  if (process.env['worker_arg']) {
    worker_arg = JSON.parse(process.env['worker_arg']);
    range = worker_arg.range;
    shard_name = worker_arg.name;
  }
  var from_uri      = new URI(cli.from),
      to_uri     = new URI(cli.to),
      from_client   = new elasticsearch.Client({host:from_uri.host(), requestTimeout:cli.request_timeout}),
      to_client  = new elasticsearch.Client({host:to_uri.host(), requestTimeout:cli.request_timeout}),
      from_path     = (function() { var tmp = from_uri.path().split('/'); return { index:tmp[1], type:tmp[2]}})(),
      to_path    = (function() { var tmp = to_uri.path().split('/'); return { index:tmp[1], type:tmp[2]}})(),
      processed_total        = 0;
  var scan_options = {
        index       : from_path.index,
        type        : from_path.type,
        search_type : 'scan',
        scroll      : cli.scroll,
        size        : cli.bulk,
        body        : {}
      };

  if (range) {
    _.defaults(scan_options.body, {query:{range:range}});
  }

  if (custom_indexer && custom_indexer.query) {
    scan_options.body = _.extend(scan_options.body, custom_indexer.query);
  }

  var reindexer = new Indexer();
  var bar = new ProgressBar("    " + shard_name + " reindexing [:bar] :current/:total(:percent) :elapsed :etas", {total:100, width:30});;

  reindexer.on('warning', function(warning) {
    logger.warn(warning);
  });

  reindexer.on('error', function(error) {
    logger.error(error);
  });

  reindexer.on('batch-complete', function(num_of_success) {
    bar.tick(num_of_success);
  });

  from_client.search(scan_options, function scroll_fetch(err, res) {
    if (err) {
      logger.fatal(err);
      return console.log("Scroll error:" + err);
    }
    bar.total = cli.max_docs == -1 ? res.hits.total : cli.max_docs;
    var docs = res.hits.hits;
    processed_total = processed_total + docs.length;
    if (processed_total > bar.total) {
      docs = docs.slice(0, bar.total - processed_total);
      processed_total = bar.total;
    }
    reindexer.index(docs, {
      concurrency : cli.concurrency,
      bulk        : cli.bulk,
      client      : to_client,
      indexer     : custom_indexer ? custom_indexer.index : null,
      index       : to_path.index,
      type        : to_path.type
    }, function(err) {
      if (err) {
        logger.fatal(err);
        return console.log("Reindex error: " + err);
      }
      if (processed_total < bar.total) {
        from_client.scroll({
          scrollId : res._scroll_id,
          scroll : cli.scroll
        }, scroll_fetch);
      } else {
        console.log("\n    Total " + processed_total + " documents have been reindexed!");
        process.exit();
      }
    });
  });
}