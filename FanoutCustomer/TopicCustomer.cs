using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FanoutCustomer
{
    public class TopicCustomer
    {
        string exchangeName = "ExchangeTopic";

        IConnection connection;
        IModel channel;
        public void Start()
        {

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";
            factory.Port = 5672;
            factory.VirtualHost = "/demo";
            factory.UserName = "wqc";
            factory.Password = "wqc";
            connection = factory.CreateConnection();
            {
                channel = connection.CreateModel();
                {
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: false, autoDelete: false, arguments: null);

                    String queueName = channel.QueueDeclare().QueueName;

                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "#.red", arguments: null);

                    EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        Byte[] body = ea.Body;
                        String message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0}", message);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };

                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                }
            }
        }
        public void Stop()
        {
            channel.Dispose();
            channel.Close();
            connection.Dispose();
            connection.Close();
        }
    }
}
