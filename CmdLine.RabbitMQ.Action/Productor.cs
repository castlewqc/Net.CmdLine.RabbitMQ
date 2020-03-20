using RabbitMQ.Client;
using RabbitMQ.Client.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Action
{
    /// <summary>
    /// rabbitmq 生成者 局限的保障消息的顺序性
    /// rabbitmq 支持最多一次和最少一次，不支持恰好一次
    /// </summary>
    class Productor
    {
        private static readonly string EXCHANGE_NAME = "direct_exchange_name";
        private static readonly string ALTERNATE_EXCHANGE_NAME = "alternate_exchange_name";
        private static readonly string QUEUE_NAME = "direct_queue_name";
        private static readonly string UNROUTE_QUEUE_NAME = "unrouted_queue_name";
        private static readonly string DLX_EXCHANGE_NAME = "dlx_exchange_name";
        private static readonly string DLX_QUEUE_NAME = "dlx_queue_name";
        private static readonly string DLX_ROUTE_KEY = "dlx_route_key";
        private static readonly string ROUTE_KEY = "direct_route_key";
        public void CreateNow()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = "localhost";
            factory.Port = 5672;
            factory.VirtualHost = "/demo";
            factory.UserName = "wqc";
            factory.Password = "wqc";

            IConnection connection = factory.CreateConnection();
            IModel channel = connection.CreateModel();


            Dictionary<string, object> arguments_exchange = new Dictionary<string, object>
            {
                { "alternate-exchange", ALTERNATE_EXCHANGE_NAME }
            };
            // 设置消息的TTL 两种方式: 聲明queue的時候；發送消息的時候 兩者取其小
            // 第一種方式：rabbitmq 定期從頭掃描是否有過期的消息
            // 第二種方式：rabbitmq 掃描頭部，頭部消息不過期，不會掃描下面的消息
            Dictionary<string, object> arguments_queue = new Dictionary<string, object>
            {
                  { "x-message-ttl", 6000 }, // 0: 可以直接将消息投递给消费者，不然立即丢弃，部分替代immediate
                  { "x-expires", 1800000 }, // 队列TTL 不能设置为0 
                  { "x-max-length", 10 }, //
                  { "x-dead-letter-exchange", DLX_EXCHANGE_NAME },
                  { "x-dead-letter-routing-key", DLX_ROUTE_KEY }, // 不声明的情况下使用原routekey
                  { "x-max-priority", 10 } // 设置队列的最高优先级，最低优先级为0
            };
            // 设置EXCHANGE_NAME的备份交换器，备份交换器收到消息后存入相应的备份队列
            // 这种方式和mandatory:true 二选一
            // 当交换器已存在，又声明了配置不一致的交换器 >> 发生错误
            // 刪除隊列（測試用）
            channel.QueueDelete(QUEUE_NAME, false, false);

            channel.ExchangeDeclare(DLX_EXCHANGE_NAME, ExchangeType.Direct, true, false, null);
            channel.QueueDeclare(DLX_QUEUE_NAME, true, false, false, null);
            channel.QueueBind(DLX_QUEUE_NAME, DLX_EXCHANGE_NAME, DLX_ROUTE_KEY, null);

            channel.ExchangeDeclare(ALTERNATE_EXCHANGE_NAME, ExchangeType.Fanout, true, false, null);
            channel.QueueDeclare(UNROUTE_QUEUE_NAME, true, false, false, null);
            channel.QueueBind(UNROUTE_QUEUE_NAME, ALTERNATE_EXCHANGE_NAME, "no_route_key", null);

            // 消息找不到匹配的队列 备份交换器
            channel.ExchangeDeclare(EXCHANGE_NAME, ExchangeType.Direct, true, false, arguments_exchange);
            // 消息死信 死信队列 （消息被拒绝 reject/nack && requeue == false; 消息过期; 队列达到最大长度;）
            channel.QueueDeclare(QUEUE_NAME, true, false, false, arguments_queue);
            channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTE_KEY, null);

            // 自动生成的队列名称（Rabbitmq 服务上会自动创建一个队列）
            // string autoMiticQueueName = channel.QueueDeclare().QueueName;

            channel.BasicReturn += Channel_BasicReturn;

            // 当开启生成者确认模式后，rabbitmq server 收到消息（需要持久化的时候持久化后） 发送ack/nck给生产者
            channel.ConfirmSelect();
            channel.BasicNacks += Channel_BasicNacks;
            channel.BasicAcks += Channel_BasicAcks;
            /**
             * QOS quality of service 服务质量
             * 第一个参数 所能接收未确认消息的总体大小, 单位为B 一般必须为0,代表没有上线
             * 第二个参数 不应答的消息个数，预取个数
             * 第三个参数 当channel消费多个队列时,预取个数是否针对信道上所有队列 无特殊需要，使用默认配置false
             * 消息公平分发 对get模式无效
             * 发送前调用
             */
            channel.BasicQos(0, 1, false);

            Task.Run(() =>
            {
                int i = 0;
                while (i++ < 100)
                {
                    /**
                     * mandatory （命令的; 托管的; 强制的）看是否有匹配的队列，没有则返回给生产者
                     * immediate 看所有匹配队列都没有消费者，则返回给生成者
                     */

                    IBasicProperties basicProperties = channel.CreateBasicProperties();
                    basicProperties.Persistent = true; // 投递模式  basicProperties.DeliveryMode = 2（持久化）;
                    basicProperties.ContentType = "text/plain";
                    basicProperties.Priority = 1;
                    basicProperties.UserId = "wqc";
                    Dictionary<string, object> headers = new Dictionary<string, object>
                    {
                        { "location", "here" },
                        { "time", "today" }
                    };
                    ulong nextSeqNo = channel.NextPublishSeqNo;
                    basicProperties.Headers = headers;
                    basicProperties.Expiration = "3000"; // 消息的过期事件设置
                    basicProperties.Priority = Convert.ToByte(nextSeqNo % 10); // 优先级高的消息可能被优先的消费
                    channel.BasicPublish(EXCHANGE_NAME, ROUTE_KEY, mandatory: true, basicProperties, Encoding.UTF8.GetBytes(i + ""));
                    confirmSet.Add(nextSeqNo);
                    // channel.BasicPublish(EXCHANGE_NAME, "noroute_key", mandatory: true, basicProperties, Encoding.UTF8.GetBytes(i + ""));
                    // channel.BasicPublish(EXCHANGE_NAME, "noroute_key", mandatory: false, basicProperties, Encoding.UTF8.GetBytes(i + ""));
                    Console.WriteLine("send message " + i);
                    Task.Delay(1000).Wait();

                }
            }).Wait();

            channel.Close();
            connection.Close();

        }

        private SortedSet<ulong> confirmSet = new SortedSet<ulong>();
        private void Channel_BasicAcks(object sender, global::RabbitMQ.Client.Events.BasicAckEventArgs e)
        {
            Console.WriteLine("Channel_BasicAcks");
            ulong deliveryTag = e.DeliveryTag;
            if (e.Multiple)
            {
                confirmSet.RemoveWhere(seqNo => seqNo <= deliveryTag);
            }
            else
            {
                confirmSet.Remove(deliveryTag);
            }
        }

        private void Channel_BasicNacks(object sender, global::RabbitMQ.Client.Events.BasicNackEventArgs e)
        {
            Console.WriteLine("Channel_BasicNacks");
            ulong deliveryTag = e.DeliveryTag;
            if (e.Multiple)
            {
                confirmSet.RemoveWhere(seqNo => seqNo <= deliveryTag);
            }
            else
            {
                confirmSet.Remove(deliveryTag);
            }
            Console.WriteLine("todo 重发消息");
        }

        // <summary>
        // mandatory:true  发送消息未匹配到队列的返回处理事件
        // mandatory:false 消息丢弃
        // </summary>
        // <param name="sender"></param>
        // <param name="e"></param>
        private void Channel_BasicReturn(object sender, global::RabbitMQ.Client.Events.BasicReturnEventArgs e)
        {
            Console.WriteLine("Channel_BasicReturn " + Encoding.UTF8.GetString(e.Body));
        }


        // <summary>
        // 多个生产者同时运行，往统一队列中存放消息 
        // </summary>
        public void ConcurrentCreateNow()
        {
            Task.Run(() => CreateNow());
            Task.Run(() => CreateNow());
        }
    }
}
