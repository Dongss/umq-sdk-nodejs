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

function getmessage(s, topic) {
  let succCount = 0;
  let f = () => {
    s.getMessage(topic, 10, 1).then((msgs) => {
      console.log(msgs)
      for (let i = 0; i < msgs.length; i++) {
        if (messagesExpectToRecv[msgs[i].content]) {
          succCount++;
        }
      }
      console.log(msgs)
      return s.ackMessage(topic, msgs.map(function (m) {
        return m.messageID;
      }));
    }).then(() => {
      if (succCount == pubsubTestMessageCount) {
        console.log("yes, done");
        return
      }
      setImmediate(f);
    }).catch((e) => {
      console.error(e);
      process.exit(-1);
    })
  }
  f();
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
  let c = client.createConsumer(config.ConsumerId, config.ConsumerToken);
  let s = client.createSubscription(config.ConsumerId, config.ConsumerToken, config.Topic, 10);
  sub(s);
  pub(p);
}

testPubSub();

