const { EventHubClient, EventPosition, delay } = require('@azure/event-hubs')
const { senderConnectionString } = require('../config')
const entityPath = 'Event hub name'
let client

async function main() {
  client =EventHubClient.createFromConnectionString(senderConnectionString, entityPath)
  const ids = await client.getPartitionIds();
  for (let i = 0; i < 1; i++) {
    const onMessage = (eventData) => {
      console.log("### Actual message:", eventData.body)
    }

    const onError = (err) => {
      console.log(">>>>> Error occurred: ", err);
    }

    const rcvrHandler = client.receive(ids[i], onMessage, onError, {
      enableReceiverRuntimeMetric: true,
      eventPosition: EventPosition.fromEnqueuedTime(Date.now())
    });

    // giving some time for receiver setup to complete. This will make sure that the
    // receiver can receive the newly sent message from now onwards.
    
    await delay(3000);
    
    console.log("***********Created receiver %d", i);
  }
}

main().then(() => {
  return client.close();
}).catch((err) => {
  console.log("error: ", err);
});