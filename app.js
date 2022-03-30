const express = require('express')
const db = require('./db')
const app = express()
const port = process.env.PORT || 3000

const amqplib = require('amqplib');
const amqpUrl = process.env.AMQP_URL || 'amqp://localhost:5673';

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  if(req.method == 'OPTIONS') {
    res.header("Access-Control-Allow-Methods", "PUT, POST, PATCH, DELETE, GET");
    return res.status(200).json({});
  }
  next();

})
app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.post('/new_ride', async (req, res) => {
  //Initialising the connection
  const connection = await amqplib.connect(amqpUrl, 'heartbeat=60');
  const channel = await connection.createChannel();
  
  try {
    console.log('Publishing');
    const exchange = 'user.got_ride';
    const queue = 'ride_match';
    const routingKey = 'user_ride';
    
    //Asserting the exchange
    await channel.assertExchange(exchange, 'direct', {durable: true});
    await channel.assertQueue(queue, {durable: true});
    await channel.bindQueue(queue, exchange, routingKey);


    //Publishing the message
    const msg = {'pickup': 'yoyo', 'destination': 'yoyoma', 'time':Math.floor(Math.random() * 100), 'cost': 150, 'seats':4};
    await channel.publish(exchange, routingKey, Buffer.from(req.body.msg));
    //await channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(msg)));
    console.log('Message published');
    res.send('Message published');
  } catch(e) {
    //Catching the error
    console.error('Error in publishing message', e);
    res.send('Error in publishing message');
  } finally {
    //Closing the channel and connection if available
    console.info('Closing channel and connection if available');
    await channel.close();
    await connection.close();
    console.info('Channel and connection closed');
    res.send('Channel and connection closed');
  }
  process.exit(0);
})

app.post('/new_ride_matching_consumer', (req, res) => {
  console.log("Message processed for the container");
      console.log(process.env.CONSUMER_ID);
      console.log(process.env.PRODUCER_ADDRESS);
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})  