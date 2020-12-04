const { Client } = require('pg');
const { EventHubClient, EventPosition } = require('@azure/event-hubs');
const { connectionString } = require('./config');
const { db_host, db_user, db_database, db_password, db_port } = require('./config');

let json_db_batch_blender = require('./plc_configs/BD_Batch_Blender.json');
let json_accumeter_ovation_continuous_blender = require('./plc_configs/Accumeter_Ovation_Continuous_Blender.json');
let json_gh_f_gravimetric_additive_feeder = require('./plc_configs/GH-F_Gravimetric_Additive_Feeder.json');
let json_gh_gravimetric_extrusion_control_hopper = require('./plc_configs/GH_Gravimetric_Extrusion_Control_Hopper.json');
let json_ngx_dryer = require('./plc_configs/NGX_Dryer.json');
let json_ngx_nomad_dryer = require('./plc_configs/NGX_Nomad_Dryer.json');
let json_t50_central_granulator = require('./plc_configs/T50_Central_Granulator.json');
let json_vtc_plus_conveying_system = require('./plc_configs/VTC_Plus_Conveying_System.json');

let json_machines = [
  json_db_batch_blender,                        // 1
  json_accumeter_ovation_continuous_blender,    // 2
  json_gh_gravimetric_extrusion_control_hopper, // 3
  json_gh_f_gravimetric_additive_feeder,        // 4
  json_vtc_plus_conveying_system,               // 5
  json_ngx_dryer,                               // 6
  json_ngx_nomad_dryer,                         // 7
  json_t50_central_granulator,                  // 8
];

let dbClient;
let offset;
const text = 'INSERT INTO device_data(device_id, customer_id, machine_id, tag_id, timestamp, values) VALUES($1, $2, $3, $4, $5, $6) RETURNING *';
const text2 = 'SELECT * FROM devices WHERE serial_number = $1';

var printError = function (err) {
  console.log(err.message);
};

var printMessage = async function (message) {
  // console.log('Telemetry received: ');
  const deviceId = message.annotations["iothub-connection-device-id"];
  
  let customerId = 0;
  const res = await dbClient.query(text2, [deviceId]);

  if(res.rows.length > 0) {
    customerId = res.rows[0].company_id
    machineId = res.rows[0].machine_id
  }
  if(machineId == 1 || machineId == 2 || machineId == 3 || machineId == 4) {
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
          let type = json_machines[machineId - 1].plctags[val.id - 1].type;
          val.values.push(getTagValue(message.body, offset, byteOfElement, type));
        }
        let queryValues = [deviceId, customerId, machineId, val.id, group.timestamp, JSON.stringify(val.values)];
        await dbClient.query(text, queryValues);
        // console.log(queryValues);
        group.values.push(val);
      }
      obj.groups.push(group);
    }
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

function getTagValue(buff, start, len, type = 'int32') {
  let slicedBuff = buff.slice(start, start + len);
  let ret = 0;
  offset += len;
  if(type === 'uint32') {
    return slicedBuff.readUInt32BE();
  } else if(type === 'int16') {
    return slicedBuff.readInt16BE();
  } else if(type === 'bool') {
    var t = slicedBuff.readUInt16BE();
    return !!(t & 0xFFFF);
  } else if(type === 'float') {
    return slicedBuff.readFloatBE();
  }
  return ret;
}

module.exports = {
  start: async function() {
    try {
      dbClient = new Client({
        user: db_user,
        host: db_host,
        database: db_database,
        password: db_password,
        port: db_port,
          ssl: {
            rejectUnauthorized: false
          },
      })
      await dbClient.connect();

      console.log("DB connected !");
    } catch (error) {
      console.log("DB connection failed !");
      console.log(error)
    }

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