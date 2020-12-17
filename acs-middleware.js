const { Client } = require('pg');
const { EventHubClient, EventPosition } = require('@azure/event-hubs');
const { connectionString } = require('./config');
const { db_host, db_user, db_database, db_password, db_port } = require('./config');

let json_machines;

// let json_machines = [
//   json_db_batch_blender,                        // 1
//   json_accumeter_ovation_continuous_blender,    // 2
//   json_gh_gravimetric_extrusion_control_hopper, // 3
//   json_gh_f_gravimetric_additive_feeder,        // 4
//   json_vtc_plus_conveying_system,               // 5
//   json_ngx_dryer,                               // 6
//   json_ngx_nomad_dryer,                         // 7
//   json_t50_central_granulator,                  // 8
//   json_gp_portable_chiller,                     // 9
//   json_he_central_chiller,                      // 10
//   json_truetemp_tcu,                            // 11
// ];

let dbClient;
let offset;
const text = 'INSERT INTO device_data(device_id, customer_id, machine_id, tag_id, timestamp, values) VALUES($1, $2, $3, $4, $5, $6) RETURNING *';
const text2 = 'SELECT * FROM devices WHERE serial_number = $1';

var printError = function (err) {
  console.log(err.message);
};

var printMessage = async function (message) {
  var deviceId = message.annotations["iothub-connection-device-id"];
  
  // if (deviceId == 'TESTACS157') deviceId = 1234567157;   // BD Batch Blender
  // if (deviceId == 'TESTACS157') deviceId = 1234568157;  // GH Gravimetric Extrusion Control Hopper
  // if (deviceId == 'TESTACS157') deviceId = 2234567157;  // Accumeter Ovation Continuous Blender
  if (deviceId == 'TESTACS157') deviceId = 4234567157;  // GH-F Gravimetric Additive Feeder
  // if (deviceId == 'TESTACS157') deviceId = 5234567157;  // VTC Plus Conveying System
  // if (deviceId == 'TESTACS157') deviceId = 6234567157;  // NGX Dryer
  // if (deviceId == 'TESTACS157') deviceId = 7234567157;  // NGX Nomad Dryer
  // if (deviceId == 'TESTACS157') deviceId = 8234567157;  // T50 Central Granulator
  // if (deviceId == 'TESTACS157') deviceId = 9234567157;  // GP Portable Chiller
  // if (deviceId == 'TESTACS157') deviceId = 10234567157;  // HE Central Chiller
  // if (deviceId == 'TESTACS157') deviceId = 11234567157;  // TrueTemp TCU
  
  let customerId = 0;

  var res = null;

  try {
    res = await dbClient.query(text2, [deviceId]);
  } catch (error) {
    console.log("Device not found");
    console.log(error)
    return;
  }

  if(res && res.rows.length > 0) {
    customerId = res.rows[0].company_id
    machineId = res.rows[0].machine_id

    if(!machineId) {
      console.log(`Machine is not assigned to device ${deviceId}`);
      return;
    }
  }
  else {
    console.log(`Device ${deviceId} not found`);
    return;
  }

  if(!Buffer.isBuffer(message.body)) {
    console.log(message.body);
    return
  }

  var commandNumber = converter(message.body, 0, 1);
  if(commandNumber == 245) {
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

        if(val.id == 250) {
          return;
        }

        val.status = converter(message.body, offset, 1);  //14
        val.values = [];
        let numOfElements = converter(message.body, offset, 1); //15
        let byteOfElement = converter(message.body, offset, 1); //16
        for(let i = 0; i < numOfElements; i++) {
          let plctag = json_machines[machineId - 1].full_json.plctags.find((tag) => {
            return tag.id == val.id
          })
          if(plctag) {
            let type = plctag.type;
            val.values.push(getTagValue(message.body, offset, byteOfElement, type));
          } else {
            console.log("Can't find tag");
          }
        }
        let queryValues = [deviceId, customerId, machineId, val.id, group.timestamp, JSON.stringify(val.values)];
        try {
          await dbClient.query(text, queryValues);

          console.log({
            "deviceId": deviceId,
            "machineId": machineId,
            "tagId": val.id,
            "values": val.values
          });
          group.values.push(val);
        } catch (error) {
          console.log("Inserting into database failed.")
          console.log(error)
        }
      }
      obj.groups.push(group);
    }
  }
};

function converter(buff, start, len) {
  let slicedBuff = buff.slice(start, start + len);
  let ret;
  if(len === 1) {
    ret = slicedBuff.readUInt8();
  } else if(len === 4) {
    ret = slicedBuff.readUInt32BE()
  }
  offset += len;
  return ret;
}

function getTagValue(buff, start, len, type = 'int32') {
  let slicedBuff = buff.slice(start, start + len);
  let ret = 0;
  offset += len;

  if(type === 'bool') {
    return !!(slicedBuff.readUInt8() && 0xFF);
  }

  if(len === 1) {
    return slicedBuff.readUInt8();
  }

  if(type === 'int16') {
    return slicedBuff.readInt16BE();
  } if(type === 'float') {
    return slicedBuff.readFloatBE();
  } else if(type == 'uint32') {
    return slicedBuff.readUInt32BE();
  }
  
  return ret;
}

async function getPlcConfigs() {
  try {
    const res = await dbClient.query('SELECT * FROM machines');

    return res.rows;
  } catch (error) {
    return false;
  }
}

module.exports = {
  start: async function() {
    try {
      dbClient = new Client({
        user: db_user,
        host: db_host,
        database: db_database,
        password: db_password,
        port: db_port
      })
      await dbClient.connect();

      console.log("DB connected !");
    } catch (error) {
      console.log("DB connection failed !");
      console.log(error)
    }

    json_machines = await getPlcConfigs();

    if(!json_machines) {
      console.log("Plc configs are not available.");
    } else {

      let db_batch_blender_plctags = [];

      json_machines[0].full_json.plctags.forEach(function(plctag) {
        if(plctag.id == 12) {
          plctag.dependents.forEach((dependent) => {
            db_batch_blender_plctags.push(dependent)
          })
        } else {
          db_batch_blender_plctags.push(plctag)
        }
      })

      json_machines[0].full_json.plctags = db_batch_blender_plctags;
    }
    
    console.log(json_machines[0].full_json);
    
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