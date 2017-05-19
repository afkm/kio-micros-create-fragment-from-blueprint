#!/usr/bin/env node

'use strict';

const Path = require('path');
const _ = require('lodash');
const async = require('async');
const cuid = require('cuid');

var neo4j = require('neo4j-driver').v1;
var driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "lalala"));

driver.onCompleted = function() {
    console.log('Successfully connected to Neo4J');
};

driver.onError = function(error) {
    console.log('Neo4J Driver instantiation failed', error);
};

var session = driver.session();



var listener = require('seneca')()
    .use('seneca-amqp-transport')
    .add('cmd:createFragment,fromBlueprint:*,asChildOf:*,title:*', function(message, done) {

        // get node plus all (bi-directional) related nodes, except 'parent' nodes
        var queryString = "MATCH (startNode:Blueprint {cuid:'" + message.fromBlueprint + "'}) -[relation]- (childNode) WHERE NOT (startNode)<-[:USES]-(childNode) RETURN startNode, childNode, relation ORDER BY relation.nodeOrder";
        console.log(queryString);

        session
            .run(queryString)

            .then(function(result) {

                var newNodeCuid = null;

                async.eachSeries(result.records, function(record, callback) {

                    console.log("newNodeCuid: " + newNodeCuid + ", record: " + result.records);

                    if (newNodeCuid == null) {
                        // Create Fragment node
                        newNodeCuid = cuid();
                        var msg = "cmd:addNode,cuid:" + newNodeCuid + ",nodeType:Fragment,nodeTitle:" + message.title + ",linkOut:" + message.asChildOf + ",linkType:USES,linkProps:{}";
                        listener.act(msg, (err, result) => {
                            if (err) {
                                throw err;
                            }
                        });
                        // Link Fragment to Blueprint node
                        var msg = "cmd:linkNode,cuid:" + newNodeCuid + ",linkTo:" + message.fromBlueprint + ",linkType:BLUEPRINT_INSTANCE,linkProps:{}";
                        listener.act(msg, (err, result) => {
                            if (err) {
                                throw err;
                            }
                        });
                    }

                    // Either set start node modifier...
                    if (record.get('childNode').labels.includes('Modifier')) {
                        var msg = "cmd:linkNode,cuid:" + record.get('childNode').properties.cuid + ",linkTo:" + newNodeCuid + ",linkType:MODIFIES,linkProps:{}";
                        listener.act(msg, (err, result) => {
                            if (err) {
                                throw err;
                            }
                        });
                        return callback();

                        // ...or traverse deeper
                    } else {
                        var msg = "cmd:createNode,fromNode:" + record.get('childNode').properties.cuid + ",asChildOf:" + newNodeCuid;
                        listener.act(msg, (err, result) => {
                            if (err) {
                                throw err;
                            }
                            return callback();
                        });
                    }


                }, function(err) {

                    session.close();

                    if (err) {
                        console.error(err);
                        return;
                    }

                    return done(null, newNodeCuid);
                });
            })

            .catch(function(error) {
                console.log(error);
            });
    })

    .listen({
        type: 'amqp',
        pin: 'cmd:createFragment,fromBlueprint:*,asChildOf:*,title:*',
        url: process.env.AMQP_URL
    });

// Will never be called when quitting service...
driver.close();
