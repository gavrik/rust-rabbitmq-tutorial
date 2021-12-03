/*
Tutorial Two.
Send message to the specific queue with two consumers for reading
Based on python RabbitMQ tutorial.
https://www.rabbitmq.com/tutorials/tutorial-two-python.html
*/

use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection,
    ConnectionProperties, Result,
};
use log::info;
use std::{thread, time};

const QUEUE_NAME: &str = "tutorial-two";

// Send 1k messages to the specific queue
pub fn tutorial_two_send(addr: &str) -> Result<()> {

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
                    BasicProperties::default().with_delivery_mode(2), // make message persistent
                )
                .await;
        }

    });
    Ok(())
}

// Read/Consume messages from specific queue
pub fn tutorial_two_consume(addr: &str) -> Result<()> {

    // establishing connection
    async_global_executor::block_on(async {
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        info!("Sender CONNECTED");

        // createing new channels for communication
        let channel_consume_a = conn.create_channel().await.expect("create_channel");
        let channel_consume_b = conn.create_channel().await.expect("create_channel");

        // update channels qos. fetching only one message in a time
        channel_consume_a.basic_qos(1, BasicQosOptions::default()).await.unwrap();
        channel_consume_b.basic_qos(1, BasicQosOptions::default()).await.unwrap();

        info!("{:?}", conn.status().state());

        // creating consumer A and bind it to the queue
        let mut consumer_a = channel_consume_a
            .basic_consume(
                QUEUE_NAME,
                "consumer-one",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic_consume");

        // creating consumer B and bind it to the queue
        let mut consumer_b = channel_consume_b
            .basic_consume(
                QUEUE_NAME,
                "consumer-two",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic_consume");


        // reading messages from the queue using channel_a
        async_global_executor::spawn(async move {
            info!("Bind Consumer A");
            while let Some(delivery) = consumer_a.next().await {
                if let Ok(delivery) = delivery {
                    let (_, delivery) = delivery;
                    // print message body
                    info!("Consumer A - {:?}", String::from_utf8(delivery.data).unwrap());
                    // ack message (remove message from the queue)
                    delivery
                        .acker
                        .ack(BasicAckOptions::default())
                        .await
                        .expect("ack");
                }
            }
        }).detach();

        // reading messages from the queue using channel_b
        async_global_executor::spawn(async move {
            info!("Bind Consumer B");
            while let Some(delivery) = consumer_b.next().await {
                if let Ok(delivery) = delivery {
                    let (_, delivery) = delivery;
                    // print message body
                    info!("Consumer B - {:?}", String::from_utf8(delivery.data).unwrap());
                    // ack message (remove message from the queue)
                    delivery
                        .acker
                        .ack(BasicAckOptions::default())
                        .await
                        .expect("ack");
                }
            }
        }).detach();
    });

    thread::sleep(time::Duration::from_secs(40));
    Ok(())

}
