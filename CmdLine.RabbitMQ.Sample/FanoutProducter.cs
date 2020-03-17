using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Sample
{
    /**
     * 散开
     */ 
    public class FanoutProducter
    {
       ConcurrentBag<string> bag = new ConcurrentBag<string>();

     
        private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
        string exchangeName = "Exchange";
        string message = "Hello Exchage";
        
        public void Sent()
        {
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            factory.Port = 5672;
            factory.VirtualHost = "/demo";
            factory.UserName = "wqc";
            factory.Password = "wqc";
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Fanout, durable: false, autoDelete: false, arguments: null);
                    IBasicProperties properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    Task.Run(() =>
                    {
                        while (true)
                        {
                            for (int i = 0; i < 10000; i++)
                            {
                                Byte[] body = Encoding.UTF8.GetBytes(message + i);
                                channel.BasicPublish(exchange: exchangeName, routingKey: "", basicProperties: properties, body: body);
                            }
                            Thread.Sleep(100);
                        }
                    }).Wait();

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
