const amqplib = require('amqplib');
const MongoClient = require('mongodb').MongoClient;
const amqpUrl = process.env.AMQP_URL || 'amqp://localhost:5673';
//const url = `mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MONGO_HOSTNAME}:${MONGO_PORT}/${MONGO_DB}?authSource=admin`;
const url = `mongodb://shreyas:shreyas123@127.0.0.1:27017/cc_hack_2?authSource=admin`;

async function processMessage(msg) {
  console.log(msg.content.toString(), 'Calling ride-sharing service API here');
}

(async () => {
    const connection = await amqplib.connect(amqpUrl, "heartbeat=60");
    const channel = await connection.createChannel();
    channel.prefetch(10);

    const queue = 'ride_match';
    process.once('SIGINT', async () => { 
      console.log('got sigint, closing connection');
      await channel.close();
      await connection.close(); 
      process.exit(0);
    });

    await channel.assertQueue(queue, {durable: true});
    await channel.consume(queue, async (msg) => {
      console.log('processing messages');      
      await processMessage(msg);
      await channel.ack(msg);

      console.log("Message processed for the container");
      console.log(process.env.CONSUMER_ID);
      console.log(process.env.PRODUCER_ADDRESS);
      MongoClient.connect(url, (err, db) => {
        if(err)
          throw err;
        db.collection('cc_hack_2_collection').insertOne(msg, (err, result) => {
          if(err)
            throw err;
          console.log('Record inserted')
          db.close();
        })
      })

    }, 
    {
      noAck: false,
      //consumerTag: 'email_consumer'
      consumerTag: 'ride_match_consumer'
    });
    console.log(" [*] Waiting for messages. To exit press CTRL+C");
})();