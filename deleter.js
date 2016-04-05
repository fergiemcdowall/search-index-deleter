//deletes all references to a document from the search index

var _defaults = require('lodash.defaults');
var _flatten = require('lodash.flatten');
var _remove = require('lodash.remove')
var _uniq = require('lodash.uniq');
var a = require('async');
var skeleton = require('log-skeleton');

module.exports = function (givenOptions, callback) {

  getOptions(givenOptions, function(err, options) {
  
    var log = skeleton((options) ? options.log : undefined);
    var Deleter = {};

    Deleter.deleteBatch = function (deleteBatch, callbacky) {
      var numberDocsToDelete = 0;
      if (!options.deletable) {
        return callbacky(new Error('this index is non-deleteable- set "deletable: true" in startup options'))
      }
      if (!Array.isArray(deleteBatch)) {
        deleteBatch = [deleteBatch]
      }
      a.mapLimit(deleteBatch, 5, function (docID, callback) {
        options.indexes.get('DELETE-DOCUMENT￮' + docID, function (err, keys) {
          if (err) {
            if (err.name == 'NotFoundError') {
              return callback(null, []);
            }
          }
          else
            numberDocsToDelete++;
          return callback(err, keys);
        });
      }, function (err, results) {
        if (!err) {
          results = _flatten(results).sort()
          results = _uniq(results)
          a.mapLimit(
            // _(results).flatten().sort().uniq(true).value(),
            results,
            5,
            function (key, callback) {
              options.indexes.get(key, function (err, value) {
                var dbInstruction = {type: 'put',
                                     key: key};
                var recalibratedValue = [];
                if (key.substring(0, 2) == 'TF') {
                  recalibratedValue = _remove(value, function (n) {
                    return (deleteBatch.indexOf(n + '') == -1);
                  }).sort();
                }
                else if (key.substring(0, 2) == 'RI') {
                  recalibratedValue = value.filter(function (item) {
                    return (deleteBatch.indexOf(item[1]) == -1);
                  }).sort(function (a, b) {
                    return b[0] - a[0];
                  });
                }
                if (recalibratedValue.length == 0)
                  dbInstruction.type = 'del';
                else
                  dbInstruction.value = recalibratedValue;
                callback(err, dbInstruction);
              });
            }, function (err, dbInstructions) {
              deleteBatch.forEach(function (docID) {
                dbInstructions.push({
                  type: 'del',
                  key: 'DELETE-DOCUMENT￮' + docID
                });
              });
              options.indexes.batch(dbInstructions, function (err) {
                if (err) log.warn('Ooops!', err);
                else log.info('batch indexed!');
                // make undefined error null
                if (typeof err == undefined) err = null;
                options.indexes.get('DOCUMENT-COUNT', function (err, value) {
                  var docCount
                  if (err)  //no DOCUMENT-COUNT set- first indexing
                    docCount = 0
                  else
                    docCount = (+value - (numberDocsToDelete))
                  options.indexes.put('DOCUMENT-COUNT',
                                      docCount,
                                      function (errr) {
                                        return callbacky(err);
                                      });
                });
              });
            });
        }
        else return callbacky(err);
      });
    };

    Deleter.flush = function (callback) {
      var deleteOps = []
      options.indexes.createKeyStream({gte: '0', lte: '￮'})
        .on('data', function (data) {
          deleteOps.push({type: 'del', key: data})
        })
        .on('error', function (err) {
          log.error(err, ' failed to empty index')
        })
        .on('end', function () {
          options.indexes.batch(deleteOps, callback)
        })
    }

//    return Deleter;
    return callback(null, Deleter)
  })
};



var getOptions = function(givenOptions, callbacky) {
  const async = require('async')
  const bunyan = require('bunyan')
  const levelup = require('levelup')
  const sw = require('stopword')
  givenOptions = givenOptions || {}
  async.parallel([
    function(callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.fieldsToStore = 'all'
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = sw.getStopwords('en').sort()
      defaultOps.log = bunyan.createLogger({
        name: 'search-index',
        level: givenOptions.logLevel || defaultOps.logLevel
      })
      callback(null, defaultOps)
    },
    function(callback){
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function(err, db) {
          callback(null, db)          
        })
      }
      else {
        callback(null, null)
      }
    }
  ], function(err, results){
    var options = _defaults(givenOptions, results[0])
    if (results[1] != null) {
      options.indexes = results[1]
    }
    return callbacky(err, options)
  })
}
