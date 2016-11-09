'use strict';

var umqclient = require('umq-nodejs-sdk')
var config = require('./config');
var assert = require('assert');

var pubsubTestMessageCount = 10;
var messagesExpectToRecv = {};

function pub(p) {
  for (let i = 0; i < pubsubTestMessageCount; ++i) {
    let d = "umq test pubsub" + Date.now();
    messagesExpectToRecv[d] = 1
    p.publishMessage(config.Topic, d).then((msgId) => {
      messagesExpectToRecv[msgId] = d
      console.log("send message ", msgId)
    }).catch(err => {
      console.error(err);
      process.exit(-1);
    });
  }
}

function sub(s) {
  let succCount = 0;
  s.on("data", (message) => {
    let msgId = message.messageID;
    console.log("receive message", msgId, message.content, messagesExpectToRecv[message.content]);
    
    s.ackMessage([msgId]).then(() => {
      console.log("ack message " + msgId);
    }).catch(err => {
      console.error(err);
      process.exit(-1);
    });
    
    if (messagesExpectToRecv[message.content]) {
      succCount++;
    }
    if (succCount == pubsubTestMessageCount) {
      console.log("yes, done");
      s.close();
    }
  });
}

function testPubSub() {
  let client = umqclient.newUmqClient({
    host: config.Host,
    projectId: config.ProjectId,
    timeout: 5000,
  });
  let p = client.createProducer(config.ProducerId, config.ProducerToken); 
  let s = client.createSubscription(config.ConsumerId, config.ConsumerToken, config.Topic, 2);
  sub(s);
  pub(p);
}

testPubSub();

