using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CmdLine.Receiver.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            //多消费者 循环Round-Robin
            //Task.Run(() => Receive("001"));
            //Task.Run(() => Receive("002"));

            Console.ReadKey();

        }
        //消息确认 message acknowledgments


        private static void Receive(string name)
        {
            //1.实例化连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };
            //2. 建立
            using (var connection = factory.CreateConnection())
            {
                //3. 创建
                using (var channel = connection.CreateModel())
                {
                    //4. 申明队列
                    channel.QueueDeclare(queue: "sample", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    //5. 构造消
                    var consumer = new EventingBasicConsumer(channel);
                    //6. 绑定消息接收后的事件委托
                    consumer.Received += (model, ea) =>
                    {
                        var message = Encoding.UTF8.GetString(ea.Body);
                        Thread.Sleep(3000);//耗时操作
                        Console.WriteLine(string.Format("[{0}]Received Message: {1}", name, message));
                        // 8. 发送消息确认信号（手动消息确认） delivery 传递 交付
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };
                    //7. 启动消费者
                    //autoAck:true；自动进行消息确认，当消费端接收到消息后，就自动发送ack信号，不管消息是否正确处理完毕
                    //autoAck:false；关闭自动消息确认，通过调用BasicAck方法手动进行消息确认
                    channel.BasicConsume(queue: "sample", autoAck: false, consumer: consumer);
                    Console.ReadKey();
                }
            }
        }

        private static void ReceiveExchange()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            string exchangeName = "sampleExchange";
            string queueName = "sample";
            string routeKeyName = "RouteKey";
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);

                    //声明队列
                    channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                    //将队列和交换机绑定
                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: routeKeyName, arguments: null);

                    //定义接收消息的消费者逻辑
                    EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        Byte[] body = ea.Body;
                        String message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0}", message);
                    };

                    //将消费者和队列绑定
                    channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();

                }
            }

        }
    }
}
