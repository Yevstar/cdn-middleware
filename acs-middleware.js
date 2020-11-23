const { Client } = require('pg');
var { EventHubClient, EventPosition } = require('@azure/event-hubs');

var connectionString = 'HostName=ACSIoTHubProd.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=nXC/FXcVemxeOuZ/jN/XxeqscdeazqL8WJiy6PTkwTQ=';

var printError = function (err) {
  console.log(err.message);
};

function converter(buff, start, len, isValue = false) {
  let slicedBuff = buff.slice(start, start + len);
  let ret;
  if(len === 1) {
    ret = isValue ? slicedBuff.readInt8() : slicedBuff.readUInt8();
  } else if(len === 4) {
    ret = isValue ? slicedBuff.readInt32BE() : slicedBuff.readUInt32BE()
  }
  offset += len;
  return ret;
}

var printMessage = async function (message) {
  console.log('Telemetry received: ');
};

module.exports = {
  init: async function() {
    dbClient = new Client({
      user: 'postgres',
      host: '157.230.210.3',
      database: 'acs',
      password: 'L0nd0n',
      port: 5432,
        ssl: {
          rejectUnauthorized: false
        },
    })
    await dbClient.connect();
  },

  start: function () {
    var ehClient;

    EventHubClient.createFromIotHubConnectionString(connectionString).then(function (client) {
      console.log("Successully created the EventHub Client from iothub connection string.");
      ehClient = client;
      return ehClient.getPartitionIds();
    }).then(function (ids) {
      console.log("The partition ids are: ", ids);

      return ids.map(function (id) {
        return ehClient.receive(id, printMessage, printError, { eventPosition: EventPosition.fromEnqueuedTime(Date.now()) });
      });
    }).catch(printError);
  }
};