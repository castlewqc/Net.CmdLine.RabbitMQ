using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Sample
{
    public class SampleMQ
    {
        public SampleMQ()
        {
        }
        //消息持久化 设置队列durable:true 并发送端指定Persistent = true 最好两边都设置durable:true
        public void Sent(string msg)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    //durable 持久化队列 Persistent 持久化消息
                    //exclusive 专用的
                    //channel.QueueDeclare(queue: "sample", durable: false, exclusive: false, autoDelete: false, arguments: null);

                    //--- 消息持久化 确保消息队列的持久化 参考 https://www.rabbitmq.com/confirms.html
                    channel.QueueDeclare(queue: "sample", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    //---

                    //公平分发
                    //设置prefetchCount : 1来告知RabbitMQ，在未收到消费端的消息确认时，不再分发消息，也就确保了当消费端处于忙碌状态时，不会再给消息
                    //size 服务器给消费者传递数据大小上限 count 服务器给消费者传递数量上限 global 此设置是否应用于整个channel而不是每个consumer
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                    string message = msg;
                    byte[] body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "", routingKey: "sample", basicProperties: null, body: body);
                    Console.WriteLine("Sent Message: " + message);
                }
            }
        }
        public void Send(string message)
        {
            var factory = new ConnectionFactory() {
                HostName = "localhost", VirtualHost = "/demo",UserName="wqc",Password="wqc",Port=5672 };
            string exchangeName = "ExchangeDirect";
            string queueName = "sample";
            string routeKeyName = "RouteKey";
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: false, autoDelete: false, arguments: null);
                    Task.Run(() =>
                    {
                        //while (true)
                        {
                            for (int i = 0; i < 10; i++)
                            {
                                Byte[] body = Encoding.UTF8.GetBytes(message + i);
                                channel.BasicPublish(exchange: exchangeName, routingKey: routeKeyName, body: body);
                                Console.WriteLine(" [x] Sent {0}", message);
                            }
                            Thread.Sleep(100);
                        }
                    }).Wait();

                  
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
      

       
    }
   
}
