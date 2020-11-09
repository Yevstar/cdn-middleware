'use strict';

var connectionString = 'HostName=ACSIoTHubProd.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=nXC/FXcVemxeOuZ/jN/XxeqscdeazqL8WJiy6PTkwTQ=';

// Using the Node.js SDK for Azure Event hubs:
//   https://github.com/Azure/azure-event-hubs-node
// The sample connects to an IoT hub's Event Hubs-compatible endpoint
// to read messages sent from a device.
var { EventHubClient, EventPosition } = require('@azure/event-hubs');
const { Client } = require('pg');

let dbClient;
let offset;
const text = 'INSERT INTO blenders(blender_id, timestamp, values) VALUES($1, $2, $3) RETURNING *';

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

var printMessage = function (message) {
  console.log('Telemetry received: ');
  let groupNum = converter(message.body, 1, 4);
  let obj = {};
  obj.groups = [];
  offset = 5;
  for (var N = 0; N < groupNum; N++) {
    let group = {};
    group.timestamp = converter(message.body, offset, 4); //5
    group.values = [];
    let valCount = converter(message.body, offset, 4);  //9
    for (var M = 0; M < valCount; M++) {
      let val = {};
      val.id = converter(message.body, offset, 1);  //13
      val.status = converter(message.body, offset, 1);  //14
      val.values = [];
      let numOfElements = converter(message.body, offset, 1); //15
      let byteOfElement = converter(message.body, offset, 1); //16
      for(let i = 0; i < numOfElements; i++) {
        val.values.push(converter(message.body, offset, byteOfElement, true));
      }
      let queryValues = [val.id, group.timestamp, JSON.stringify(val.values)];
      dbClient.query(text, queryValues, (err, res) => {
        if (err) {
          console.log(err.stack)
        } else {
          console.log(res.rows[0])
        }
      })
      group.values.push(val);
    }
    obj.groups.push(group);
  }
  // console.log(JSON.stringify(obj, null, 2));
};

async function main(){
  dbClient = new Client({
    user: 'deploy@acstestdbserver',
    host: 'acstestdbserver.postgres.database.azure.com',
    database: 'acs',
    password: 'gJwsEqpdYjbDFapAH4nVTstW',
    port: 5432,
      ssl: {
        rejectUnauthorized: false,
        // ca: fs.readFileSync('/path/to/server-certificates/root.crt').toString(),
        // key: fs.readFileSync('/path/to/client-key/postgresql.key').toString(),
        // cert: fs.readFileSync('/path/to/client-certificates/postgresql.crt').toString(),
      },
  })
  await dbClient.connect();

  console.log("DB connected !");

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

main().catch((err) => {
  console.log("Error occurred: ", err);
});

