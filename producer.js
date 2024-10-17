const { Kafka } = require('./client');

async function init() {
    const producer = Kafka.producer();
    console.log("Connecting Producer");
    await producer.connect();
    console.log("Producer Connected Successfully");
    await producer.send({
        topic: 'rider-updates',
        messages: [
            {
                partition: 0,
                key: 'location-update',
                value: JSON.stringify({ name: 'Izhar', location: 'south' })
            }
        ]

    })
    await producer.disconnect();
}

init();