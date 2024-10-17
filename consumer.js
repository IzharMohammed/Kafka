const { Kafka } = require('./client'); // Import the Kafka client from the custom 'client' module

const group = process.argv[2]; // Capture the consumer group ID from command-line arguments

async function init() {
    const consumer = Kafka.consumer({ groupId: group }); // Create a Kafka consumer instance and set the groupId to the input from the terminal

    await consumer.connect(); // Connect the consumer to Kafka

    await consumer.subscribe({ // Subscribe the consumer to the 'rider-updates' topic
        topics: ['rider-updates'], // Topic to subscribe to
        fromBeginning: true        // Ensures the consumer reads messages from the beginning of the topic
    });

    await consumer.run({ // Start the consumer and process each incoming message
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => { // Callback to handle each message
            console.log( // Log the group ID, topic name, partition number, and message value
                `${group}: [${topic}]: PART:${partition}:`,
                message.value.toString() // Convert message value to string before logging
            )
        },
    })
}

init(); // Execute the async init function to start the process
