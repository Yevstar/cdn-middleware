const { Client } = require('pg');
const { EventHubClient, EventPosition } = require('@azure/event-hubs');
const { connectionString } = require('./config');

let dbClient;
let offset;
const text = 'INSERT INTO device_data(device_id, customer_id, tag_id, timestamp, values) VALUES($1, $2, $3, $4, $5) RETURNING *';
const text2 = 'SELECT * FROM devices WHERE serial_number = $1';

var printError = function (err) {
  console.log(err.message);
};

var printMessage = async function (message) {
  // console.log('Telemetry received: ');
  const deviceId = message.annotations["iothub-connection-device-id"];
  
  let customerId = 0;
  const res = await dbClient.query(text2, [deviceId]);

  console.log(res.rows[0]);
  
  if(res.rows.length > 0) {
    customerId = res.rows[0].company_id
  }
  
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
      let queryValues = [deviceId, customerId, val.id, group.timestamp, JSON.stringify(val.values)];
      await dbClient.query(text, queryValues);
      group.values.push(val);
    }
    obj.groups.push(group);
  }
  // console.log(JSON.stringify(obj, null, 2));
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

module.exports = {
  start: async function() {
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
};