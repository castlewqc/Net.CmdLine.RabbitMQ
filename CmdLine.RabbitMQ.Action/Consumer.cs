using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Action
{
    class Consumer : DefaultBasicConsumer  // 另一消费者类 EventingBasicConsumer 
    {
        private static readonly string QUEUE_NAME = "QUEUE_NAME_01";
        private static readonly string CONUMER_TAG = "CONUMER_TAG_01";
        public void Consume()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";
            factory.Port = 5672;
            factory.VirtualHost = "/demo";
            factory.UserName = "wqc";
            factory.Password = "wqc";

            IConnection connection = factory.CreateConnection();
            IModel channel = connection.CreateModel();
            this.Model = channel;
            // 同一channel的消费者也需要用唯一的消费者标签以作区分
            channel.BasicConsume(QUEUE_NAME, false, CONUMER_TAG, this);
        }

        /**
         * HandleBasicConsumeOk
HandleBasicDeliver
amq.ctag-Vf5DN1p0OTtCGucX55lvPg
1
True
EXCHANGE_NAME_01
ROUTE_KEY_01
1
HandleBasicDeliver
amq.ctag-Vf5DN1p0OTtCGucX55lvPg
2
True
EXCHANGE_NAME_01
ROUTE_KEY_01
2
         * 
         */
        public override void HandleBasicCancel(string consumerTag)
        {
            Console.WriteLine("HandleBasicCancel");
            base.HandleBasicCancel(consumerTag);

        }

        public override void HandleBasicCancelOk(string consumerTag)
        {
            Console.WriteLine("HandleBasicCancelOk");
            base.HandleBasicCancelOk(consumerTag);

        }

        public override void HandleBasicConsumeOk(string consumerTag)
        {
            Console.WriteLine("HandleBasicConsumeOk");
            base.HandleBasicConsumeOk(consumerTag);

        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            Console.WriteLine("HandleBasicDeliver");
            Console.WriteLine(consumerTag); // 当多个消费者公用一个消费处理类时,可以区分不同消费者
            Console.WriteLine(deliveryTag); // 消息编号
            Console.WriteLine(redelivered);
            Console.WriteLine(exchange);
            Console.WriteLine(routingKey);
            Console.WriteLine(Encoding.UTF8.GetString(body));
            Console.WriteLine(properties.Headers["location"]);
            if (deliveryTag > 20)
            {
                this.Model.BasicReject(deliveryTag,false); // false 消息不重新存入队列，更不会发给下一个订阅的消费者，消息直接从队列中移除
            }
            this.Model.BasicAck(deliveryTag, false); // true 消息编号之前的都ack
            
            base.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
        }

        public override void HandleModelShutdown(object model, ShutdownEventArgs reason)
        {
            Console.WriteLine("HandleModelShutdown");
            base.HandleModelShutdown(model, reason);

        }

        public override void OnCancel()
        {
            Console.WriteLine("OnCancel");
            base.OnCancel();
        }

    }
}
