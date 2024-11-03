using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

// Declare the exchange, same as in the sender
channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

// Declare a queue and bind it to the exchange
var queueName = channel.QueueDeclare().QueueName;
channel.QueueBind(queue: queueName,
                  exchange: "logs",
                  routingKey: string.Empty);

Console.WriteLine(" [*] Waiting for messages.");

// Create a consumer to receive messages from the queue
var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");

    // Simulate message processing delay
    System.Threading.Thread.Sleep(1000);

    // Manually acknowledge the message
    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
};

// Start consuming messages with manual acknowledgment
channel.BasicConsume(queue: queueName,
                     autoAck: false,   // Manual acknowledgment mode
                     consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
