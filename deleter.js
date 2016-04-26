//deletes all references to a document from the search index

var _defaults = require('lodash.defaults');
var _flatten = require('lodash.flatten');
var _remove = require('lodash.remove')
var _uniq = require('lodash.uniq');
var a = require('async');


// Constructor
module.exports = function (givenOptions, constructorCallback) {
  var options = {}
  a.series([
    // munge given options into default options, open new DB if necessary
    function(callback) {
      getOptions(givenOptions, function(err, theseOptions) {
        options = theseOptions
        return callback(err)
      })
    },
    // use options to create a new DB object
    function(callback) {
      getDeleter(options, function(err, Deleter) {
        return callback(err, Deleter)
      })
    }
  ], function (err, results) {
    return constructorCallback(err, results[1])
  })

}

// Define API calls
var getDeleter = function (options, callback) {
  var Deleter = {}
  // Delete all docs with the IDs found in deleteBatch
  Deleter.deleteBatch = function (deleteBatch, APICallback) {
    tryDeleteBatch (options, deleteBatch, function(err) {
      return APICallback(err)
    })
  }
  // Flush the db (delete all entries)
  Deleter.flush = function (APICallback) {
    flush(options, function(err) {
      return APICallback(err)
    })
  }
  return callback(null, Deleter)  
}


var deleteThisBatch = function (options, numberDocsToDelete, deleteBatch, results, callbacky) {
  results = _flatten(results).sort()
  results = _uniq(results)
  a.mapLimit(
    results,
    5,
    function (key, callback) {
      options.indexes.get(key, function (err, value) {
        var dbInstruction = {
          type: 'put',
          key: key
        };
        var recalibratedValue = [];
        if (key.substring(0, 2) == 'DF') {
          recalibratedValue = _remove(value, function (n) {
            return (deleteBatch.indexOf(n + '') == -1);
          }).sort();
        }
        else if (key.substring(0, 2) == 'TF') {
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
        if (err) options.log.warn('Ooops!', err);
        else options.log.info('batch indexed!');
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


// Try to delete the batch, throw errors if config, or formatting prevents deletion
var tryDeleteBatch = function (options, deleteBatch, callbacky) {
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
    if (err) {
      return callbacky(err)
    } else {
      deleteThisBatch(options, numberDocsToDelete, deleteBatch, results, function(err) {
        callbacky(err)
      })
    }
  });
}

// Remove all keys in DB
var flush = function(options, callback) {
  var deleteOps = []
  options.indexes.createKeyStream({gte: '0', lte: '￮'})
    .on('data', function (data) {
      deleteOps.push({type: 'del', key: data})
    })
    .on('error', function (err) {
      log.error(err, ' failed to empty index')
      return callback(err)
    })
    .on('end', function () {
      options.indexes.batch(deleteOps, callback)
    })
}

// munge given options with default options
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
