const { EventHubClient, EventPosition } = require('@azure/event-hubs')
const { connectionString } = require('../config')
const pgFormat = require('pg-format')
const Pusher = require('pusher')
const { pusherAppId, pusherKey, pusherSecret, pusherCluster, pusherUseTLS } = require('../config')
const db = require('../helpers/db')

const pusher = new Pusher({
  appId: pusherAppId,
  key: pusherKey,
  secret: pusherSecret,
  cluster: pusherCluster,
  useTLS: pusherUseTLS
})

let json_machines
let offset

const printError = function (err) {
  console.log(err.message)
}

const printMessage = async function (message) {
  let deviceId = message.annotations['iothub-connection-device-id']
  
  // if (deviceId === 'TESTACS157') deviceId = 1234567157   // BD Batch Blender
  // if (deviceId === 'TESTACS157') deviceId = 1234568157  // GH Gravimetric Extrusion Control Hopper
  // if (deviceId === 'TESTACS157') deviceId = 2234567157  // Accumeter Ovation Continuous Blender
  if (deviceId === 'TESTACS157') deviceId = 4234567157  // GH-F Gravimetric Additive Feeder
  // if (deviceId === 'TESTACS157') deviceId = 5234567157  // VTC Plus Conveying System
  // if (deviceId === 'TESTACS157') deviceId = 6234567157  // NGX Dryer
  // if (deviceId === 'TESTACS157') deviceId = 7234567157  // NGX Nomad Dryer
  // if (deviceId === 'TESTACS157') deviceId = 8234567157  // T50 Central Granulator
  // if (deviceId === 'TESTACS157') deviceId = 9234567157  // GP Portable Chiller
  // if (deviceId === 'TESTACS157') deviceId = 10234567157  // HE Central Chiller
  // if (deviceId === 'TESTACS157') deviceId = 11234567157  // TrueTemp TCU
  
  let customerId = 0

  let res = null

  try {
    res = await db.query('SELECT * FROM devices WHERE serial_number = $1', [deviceId])
  } catch (error) {
    console.log('Device not found')
    console.log(error)

    return
  }

  if (res && res.rows.length > 0) {
    customerId = res.rows[0].company_id
    machineId = res.rows[0].machine_id

    if (!machineId) {
      console.log(`Machine is not assigned to device ${deviceId}`)
      
      return
    }
  }

  if (!Buffer.isBuffer(message.body)) {
    console.log(message.body)

    return
  }

  const commandNumber = converter(message.body, 0, 1)

  if (commandNumber === 245) {
    const rowsToInsert = []
    const utilizationRowsToInsert = []
    const energyConsumptionRowsToInsert = []
    const groupNum = converter(message.body, 1, 4)
    const obj = {}

    obj.groups = []
    offset = 5

    for (let N = 0; N < groupNum; N++) {
      const group = {}

      group.timestamp = converter(message.body, offset, 4) //5
      group.values = []

      const valCount = converter(message.body, offset, 4)  //9

      for (let M = 0; M < valCount; M++) {
        const val = {}
        
        val.id = converter(message.body, offset, 1)  //13

        if (val.id === 250) {
          return
        }

        val.status = converter(message.body, offset, 1)  //14
        val.values = []
        const numOfElements = converter(message.body, offset, 1) //15
        const byteOfElement = converter(message.body, offset, 1) //16

        for (let i = 0; i < numOfElements; i++) {
          const plctag = json_machines[machineId - 1].full_json.plctags.find((tag) => {
            return tag.id === val.id
          })

          if (plctag) {
            const { type } = plctag

            val.values.push(getTagValue(message.body, offset, byteOfElement, type))
          } else {
            console.log('Can\'t find tag')
          }
        }

        const queryValues = [deviceId, customerId, machineId, val.id, group.timestamp, JSON.stringify(val.values)]
        
        // check if the tag is utilization
        try {
          res = await db.query('SELECT * FROM tags WHERE configuration_id = $1 AND tag_id = $2', [machineId, val.id])
        } catch (error) {
          console.log('Qeury from tags table failed.')

          return
        }

        if (res && res.rows.length > 0) {
          if (res.rows[0].tag_name === 'capacity_utilization') {
            utilizationRowsToInsert.push(queryValues)
          } else if (res.rows[0].tag_name === 'energy_consumption') {
            energyConsumptionRowsToInsert.push(queryValues)
          }
        } else {
          rowsToInsert.push(queryValues)
        }

        // try {
        //   res = await dbClient.query('SELECT * FROM alarm_types WHERE tag_id = $1 AND machine_id = $2', [val.id, machineId])
        //   if (res && res.rows.length > 0) {
        //     pusher.trigger('product.alarm.channel', 'alarm.created', {
        //       deviceId: deviceId,
        //       machineId: machineId,
        //       tagId: val.id,
        //       values: val.values
        //     })
        //   }

        //   console.log({
        //     'deviceId': deviceId,
        //     'machineId': machineId,
        //     'tagId': val.id,
        //     'values': val.values
        //   })
        //   group.values.push(val)
        // } catch (error) {
        //   console.log('Inserting into database failed.')
        //   console.log(error)
        // }
      }
    }

    try {

      if (rowsToInsert.length) {
        console.log(rowsToInsert)
        await db.query(pgFormat('INSERT INTO device_data(device_id, customer_id, machine_id, tag_id, timestamp, values) VALUES %L', rowsToInsert))
      }

      if (utilizationRowsToInsert.length) {
        console.log(utilizationRowsToInsert)
        await db.query(pgFormat('INSERT INTO utilizations(device_id, customer_id, machine_id, tag_id, timestamp, values) VALUES %L', utilizationRowsToInsert))
      }

      if (energyConsumptionRowsToInsert.length) {
        console.log(energyConsumptionRowsToInsert)
        await db.query(pgFormat('INSERT INTO energy_consumptions(device_id, customer_id, machine_id, tag_id, timestamp, values) VALUES %L', energyConsumptionRowsToInsert))
      }
    } catch (error) {
      console.log('Inserting into database failed.')
      console.log(error)
    }
  }
}

function converter(buff, start, len) {
  const slicedBuff = buff.slice(start, start + len)
  let ret

  if (len === 1) {
    ret = slicedBuff.readUInt8()
  } else if (len === 4) {
    ret = slicedBuff.readUInt32BE()
  }
  offset += len

  return ret
}

function getTagValue(buff, start, len, type = 'int32') {
  const slicedBuff = buff.slice(start, start + len)
  const ret = 0

  offset += len

  if (type === 'bool') {
    return !!(slicedBuff.readUInt8())
  }

  if (len === 1) {
    return slicedBuff.readUInt8()
  }

  if (type === 'int16') {
    return slicedBuff.readInt16BE()
  } if (type === 'float') {
    return slicedBuff.readFloatBE()
  } else if (type === 'uint32') {
    return slicedBuff.readUInt32BE()
  }
  
  return ret
}

async function getPlcConfigs() {
  try {
    const res = await db.query('SELECT * FROM machines')

    return res.rows
  } catch (error) {
    console.log(error)

    return false
  }
}

module.exports = {
  start: async function() {
    json_machines = await getPlcConfigs()

    if (!json_machines) {
      console.log('Plc configs are not available.')
    } else {

      const db_batch_blender_plctags = []

      json_machines[0].full_json.plctags.forEach((plctag) => {
        if (plctag.id === 12) {
          plctag.dependents.forEach((dependent) => {
            db_batch_blender_plctags.push(dependent)
          })
        } else {
          db_batch_blender_plctags.push(plctag)
        }
      })

      json_machines[0].full_json.plctags = db_batch_blender_plctags
    }
    
    let ehClient
    
    EventHubClient.createFromIotHubConnectionString(connectionString).then((client) => {
      console.log('Successully created the EventHub Client from iothub connection string.')
      ehClient = client

      return ehClient.getPartitionIds()
    }).then((ids) => {
      console.log('The partition ids are: ', ids)

      return ids.map((id) => {
        return ehClient.receive(id, printMessage, printError, { eventPosition: EventPosition.fromEnqueuedTime(Date.now()) })
      })
    }).catch(printError)
  }
}