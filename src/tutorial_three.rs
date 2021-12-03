/*
Tutorial Three.
Send message to the echange.
Based on python RabbitMQ tutorial.
https://www.rabbitmq.com/tutorials/tutorial-three-python.html
*/

use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection,
    ConnectionProperties, Result,
    ExchangeKind,
};
use log::info;
use std::{thread, time};

const QUEUE_NAME1: &str = "tutorial-three-q1";
const QUEUE_NAME2: &str = "tutorial-three-q2";
const CONSUMER_NAME1: &str = "consumer-one";
const CONSUMER_NAME2: &str = "consumer-two";
const EXCHANGE_NAME: &str = "exchange_three";

// Declare RabbitMQ topology for tutorial three
pub fn tutorial_three_topology(addr: &str) -> Result<()> {
    async_global_executor::block_on(async {
        info!("Declare RabbitMQ topology for tutorial three");

        // establishing connection
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        // createing new channel for communication
        let channel = conn.create_channel().await.expect("create_channel");

        // delaring exchange for messanges
        // ExchangeKind - type of exchange
        // - Direct
        // - Fanout
        // - Headers
        // - Topic
        let exchange = channel
            .exchange_declare(
                    EXCHANGE_NAME,
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default(),
            )
            .await
            .expect("exchange declared");

        // declare queues
        let _q1 = channel
            .queue_declare(
                QUEUE_NAME1,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue declare");

        let _q1 = channel
            .queue_declare(
                QUEUE_NAME2,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue declare");

        // bind queues to the exchange
        let _b1 = channel
            .queue_bind(
                QUEUE_NAME1,
                EXCHANGE_NAME,
                "",
                QueueBindOptions::default(),
                FieldTable::default()
            )
            .await
            .expect("queue bind 1");

        let _b2 = channel
            .queue_bind(
                QUEUE_NAME2,
                EXCHANGE_NAME,
                "",
                QueueBindOptions::default(),
                FieldTable::default()
            )
            .await
            .expect("queue bind 2");
    });
    Ok(())
}

// Send 1k messages to the exchange
pub fn tutorial_three_send(addr: &str) -> Result<()> {

    async_global_executor::block_on(async {
        info!("Generate and send 1k messages to the fanout exchange");
        // establishing connection
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        info!("Sender CONNECTED");
        
        // createing new channel for communication
        let channel_send = conn.create_channel().await.expect("create_channel");
        info!("{:?}", conn.status().state());

        for i in 0..1000 {
            let payload = format!("Hello world! message ID:{}", i);
            // publishing message through the channel
            let _confirm = channel_send
                .basic_publish(
                    EXCHANGE_NAME,
                    "",
                    BasicPublishOptions::default(),
                    payload.into_bytes(), // RabbitMQ accept only bytes in a payload
                    BasicProperties::default().with_delivery_mode(2), // make message persistent
                )
                .await;
        }
        info!("Messages were sent");
    });
    Ok(())
}

// Read/Consume messages from fanout exchage
pub fn tutorial_three_consume(addr: &str) -> Result<()> {

    // establishing connection
    async_global_executor::block_on(async {
        info!("Read/Consume messages from fanout exchage");
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect("connection error");

        info!("Consumer CONNECTED");

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
                QUEUE_NAME1,
                CONSUMER_NAME1,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("basic_consume");

        // creating consumer B and bind it to the queue
        let mut consumer_b = channel_consume_b
            .basic_consume(
                QUEUE_NAME2,
                CONSUMER_NAME2,
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