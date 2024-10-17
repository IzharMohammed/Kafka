const { Kafka } = require('./client'); // Import the Kafka client from the custom 'client' module

async function init() {
    const admin = Kafka.admin(); // Create a Kafka admin client instance
    console.log('admin connecting...'); // Log message indicating admin connection is starting
    admin.connect(); // Connect the admin client to Kafka
    console.log('admin connected!!!'); // Log message when admin successfully connects

    console.log("Creating Topic [rider-updates]"); // Log message indicating topic creation is starting
    await admin.createTopics({ // Create a new topic in Kafka with the following options:
        topics: [{
            topic: 'rider-updates', // Name of the topic
            numPartitions: 2        // Number of partitions for the topic
        }]
    })
    console.log("Topic Created Success [rider-updates]"); // Log message when topic is successfully created

    console.log("Disconnecting Admin.."); // Log message indicating the admin client is disconnecting
    admin.disconnect(); // Disconnect the admin client from Kafka
}

init(); // Execute the async init function to start the process
