var iothub = require('azure-iothub');
const { PgClient } = require('pg');

const { connectionString } = require('../config');

module.exports = {
  sendMessage: function(req, res) {
    try {
      if (!connectionString) {
        console.log('Please set the connectionString environment variable.');
        return res.status(400).json({
          message: "Connection string required!"
        })
      }

      if (!req.body.targetDevice) {
        console.log('Please give pass a target device id as argument to the script');
        return res.status(400).json({
          message: "Target device required!"
        })
      }

      var client = iothub.Client.fromConnectionString(connectionString);

      client.open(function (err) {
        if (err) {
          console.error('Could not connect: ' + err.message);
          return res.status(400).json({
            message: "Could not connect"
          })
        } else {
          console.log('Client connected');

          var message = new Message(JSON.stringify({ key : 'value' }));
          client.send(req.body.targetDevice, message, function (err) {
            if (err) {
              console.error(err.toString());
              return res.status(400).json({
                message: err.toString()
              })
            } else {
              console.log('sent c2d message');
              return res.status(200).json({
                message: "sent c2d message"
              })
            }
          });
        }
      });
    } catch (err) {
      console.log(err)

      return res.status(400).json({
        error: 'Error'
      })
    }
  }
};
