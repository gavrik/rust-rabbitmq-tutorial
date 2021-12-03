/*
Tutorial One.
Send message to the specific queue.
Based on python RabbitMQ tutorial.
https://www.rabbitmq.com/tutorials/tutorial-one-python.html
*/

use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection,
    ConnectionProperties, Result,
};
use log::info;

const QUEUE_NAME: &str = "tutorial-one";

// Send 1k messages to the specific queue
pub fn tutorial_one_send(addr: &str) -> Result<()> {

    async_global_executor::block_on(async {

        // establishing connection
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        info!("Sender CONNECTED");
        
        // createing new channel for communication
        let channel_send = conn.create_channel().await.expect("create_channel");
        info!("{:?}", conn.status().state());

        // declaring queue. skipped if queue declared already
        let queue = channel_send
            .queue_declare(
                QUEUE_NAME,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue_declare");

        info!("Declared queue {:?}", queue);

        for i in 0..1000 {
            let payload = format!("Hello world! message ID:{}", i);
            // publishing message through the channel
            let _confirm = channel_send
                .basic_publish(
                    "",
                    QUEUE_NAME,
                    BasicPublishOptions::default(),
                    payload.into_bytes(), // RabbitMQ accept only bytes in a payload
                    BasicProperties::default(),
                )
                .await;
        }

    });
    Ok(())
}

// Read/Consume messages from specific queue
pub fn tutorial_one_consume(addr: &str) -> Result<()> {

    // establishing connection
    async_global_executor::block_on(async {
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        info!("Sender CONNECTED");

        // createing new channel for communication
        let channel_consume = conn.create_channel().await.expect("create_channel");
        info!("{:?}", conn.status().state());

        // creating consumer and bind it to the queue
        let mut consumer = channel_consume
            .basic_consume(
                QUEUE_NAME,
                "consumer-one",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic_consume");

        // reading messages from the queue.
        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                let (_, delivery) = delivery;
                // print message body
                info!("{:?}", String::from_utf8(delivery.data).unwrap());
                // ack message (remove message from the queue)
                delivery
                    .acker
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("ack");
            }
        }
    });
    Ok(())

}
