'use strict';

const AWS = require('aws-sdk');

function publishMessage(snsConfig, msg, cb) {
  AWS.config.update(snsConfig.region)
  let params = {
    Message: JSON.stringify(msg),
    TopicArn: snsConfig.topicArn
  }
  // Create promise and SNS service object
  var publishTextPromise = new AWS.SNS({ apiVersion: '2010-03-31' }).publish(params).promise();

  // Handle promise's fulfilled/rejected states
  publishTextPromise.then(
    function (data) {
      cb(null, {
        topicArn: params.TopicArn,
        msgId: data.MessageId
      })
    }).catch(
      function (err) {
        cb(err, {
          topicArn: params.TopicArn,
          msgId: null
        })
      });
}

module.exports = {
  publishMessage: publishMessage
}