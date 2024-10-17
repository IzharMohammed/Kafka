const { Kafka } = require('./client'); // Import the Kafka client from the custom 'client' module
const readline = require('readline');  // Import the 'readline' module to handle user input from the terminal

// Create an interface for input and output through the terminal
const rl = readline.createInterface({
    input: process.stdin,   // Take input from stdin (terminal input)
    output: process.stdout, // Display output to stdout (terminal output)
});

async function init() {
    const producer = Kafka.producer(); // Create a Kafka producer instance
    console.log("Connecting Producer..."); // Log producer connection initiation

    await producer.connect(); // Connect the Kafka producer
    console.log("Producer Connected Successfully"); // Log successful producer connection

    rl.setPrompt('> '); // Set the command prompt to '>'
    rl.prompt();        // Display the prompt to start taking user input

    // Event listener for when a new line of input is entered
    rl.on('line', async (line) => {
        // Check if the input is empty or consists only of whitespace
        if (!line.trim()) {
            console.log("Invalid input. Please enter in the format: <riderName> <location>"); // Warn about invalid input format
            rl.prompt(); // Prompt the user again for valid input
            return;      // Exit the current iteration
        }

        // Split the input string by space and extract riderName and location
        const [riderName, location] = line.split(' ');
        if (!riderName || !location) { // Check if both riderName and location are provided
            console.log("Invalid input. Please provide both a riderName and a location."); // Warn if input is incomplete
            rl.prompt(); // Prompt the user again for valid input
            return;      // Exit the current iteration
        }

        try {
            // Send a message to the Kafka topic 'rider-updates'
            await producer.send({
                topic: 'rider-updates',   // Kafka topic to send the message to
                messages: [
                    {
                        partition: location.toLowerCase() === 'north' ? 0 : 1, // Partition based on location (north or not)
                        key: 'location-update', // Message key for tracking location updates
                        value: JSON.stringify({ name: riderName, location }), // The message value is a JSON string with riderName and location
                    },
                ],
            });
            console.log(`Sent update for ${riderName} at ${location}`); // Log successful message send
        } catch (error) {
            console.error("Error sending message:", error); // Log any errors that occur during message sending
        }

        rl.prompt(); // Prompt the user again for the next input
    });

    // Event listener for when the input interface is closed (e.g., Ctrl+C)
    rl.on('close', async () => {
        await producer.disconnect(); // Disconnect the Kafka producer
        console.log('Producer disconnected'); // Log successful disconnection
    });
}

init(); // Run the init function to start the producer and input handling
