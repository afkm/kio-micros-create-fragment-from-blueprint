#!/usr/bin/env node

'use strict';

const Path = require('path');
const _ = require('lodash');
const async = require('async');
const cuid = require('cuid');

var neo4j = require('neo4j-driver').v1;

var driver = neo4j.driver(process.env.NEO4J_HOST, neo4j.auth.basic(process.env.NEO4J_UID, process.env.NEO4J_PWD));

driver.onCompleted = function() {
    console.log('Successfully connected to Neo4J');
};

driver.onError = function(error) {
    console.log('Neo4J Driver instantiation failed', error);
};

var session = driver.session();



var listener = require('seneca')()
    .use('seneca-amqp-transport')
    .add('cmd:createFragment,fromBlueprint:*,asChildOf:*,title:*,isStartNode:*', function(message, done) {

        // get node plus all (bi-directional) related nodes, except 'parent' and sibling 'blueprint'-generated nodes (instances)
        var queryString = "MATCH (startNode {cuid:'" + message.fromBlueprint + "'}) -[relation]- (childNode) WHERE NOT (startNode)<-[:USES]-(childNode) AND NOT (startNode)<-[:BLUEPRINT_INSTANCE]-(childNode) RETURN startNode, childNode, relation ORDER BY relation.nodeOrder";

        session
            .run(queryString)
            .then(function(result) {

                var startNodeCuid = null;

                // console.log("result set length: " + result.records.length);

                async.eachSeries(result.records, function(record, callback) {

                        // console.log(`stepping into startNode ${record.get('startNode').properties.cuid} (${record.get('startNode').labels[0]}) -[${record.get('relation').properties[0]}]- childNode ${record.get('childNode').properties.cuid} (${record.get('childNode').labels[0]})`);

                        if (startNodeCuid == null) {
                            if (message.isStartNode) {
                                startNodeCuid = cuid();
                                // Create Fragment from Blueprint node
                                addNode(startNodeCuid, "Fragment", message.title, message.asChildOf);
                                // Link Fragment to Blueprint node
                                linkNode(startNodeCuid, message.fromBlueprint, "BLUEPRINT_INSTANCE");
                            } else {
                                startNodeCuid = message.asChildOf;
                            }
                        }

                        // Either set start node modifier...
                        if (record.get('childNode').labels.includes('Modifier')) {
                            linkNode(record.get('childNode').properties.cuid, startNodeCuid, "MODIFIES");
                            callback();
                        }

                        // ...or traverse deeper
                        else {

                            var childTitle = "";
                            if (_.has(record.get('childNode').properties, "title")) {
                                childTitle = _.get(record.get('childNode').properties, "title");
                            }

                            // Create node from childNode
                            var newChildNodeCuid = cuid();
                            addNode(newChildNodeCuid, record.get('childNode').labels[0], childTitle, startNodeCuid);

                            // console.log("Traversing " + record.get('childNode').properties.cuid);

                            var msg = "cmd:createFragment,fromBlueprint:" + record.get('childNode').properties.cuid + ",asChildOf:" + newChildNodeCuid + ",title:'',isStartNode:false";
                            listener.act(msg, (err, result) => {
                                if (err) {
                                    throw err;
                                }
                                return callback();
                            });
                        }

                    },
                    function(err) {
                        session.close();
                        if (err) {
                            console.error(err);
                            return;
                        }
                        var status = "Successfully created Fragment from Blueprint " + message.fromBlueprint;
                        return done(null, {
                            status
                        });
                    });
            })
            .catch(function(error) {
                console.log(error);
            });
    })

    .listen({
        type: 'amqp',
        pin: 'cmd:createFragment,fromBlueprint:*,asChildOf:*,title:*,isStartNode:*',
        url: process.env.AMQP_URL
    });


function addNode(cuid, type, title, linkTo) {
    var queryString = "MERGE (" + cuid + ":" + type + " { cuid:'" + cuid + "', title:'" + title + "' })\n";
    queryString += "WITH 1 as dummy\n";
    queryString += "MATCH (a { cuid: '" + cuid + "' }), (b { cuid: '" + linkTo + "'}) MERGE (a)<-[:USES]-(b)\n";
    // console.log("addNode: " + queryString);
    session
        .run(queryString)
        .then(function(result) {
            session.close();
            // console.log("Successfully added Node " + cuid);
        })
        .catch(function(error) {
            console.log(error);
        });
}

function linkNode(cuid, linkTo, linkType) {
    var queryString = "MATCH (a { cuid:'" + cuid + "'}), (b { cuid:'" + linkTo + "'}) MERGE (a)-[:" + linkType + "]->(b)";
    session
        .run(queryString)
        .then(function(result) {
            session.close();
            // console.log("Successfully linked Node " + cuid + " to Node " + linkTo);
        })
        .catch(function(error) {
            console.log(error);
        });
}


// Will never be called when quitting service...
driver.close();
