using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            HashSet<int> hs = new HashSet<int>();
            var smq = new SampleMQ();
            for (int i = 0; i < 100; i++)
                smq.Sent(i.ToString());
            //new TopicProductor().Sent();
            //new FanoutProducter().Sent();
            Console.ReadKey();

        }
     

     
    }
}
