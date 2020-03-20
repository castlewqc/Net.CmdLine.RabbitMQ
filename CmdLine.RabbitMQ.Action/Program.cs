using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CmdLine.RabbitMQ.Action
{
    class Program
    {
        static void Main(string[] args)
        {
            new Productor().CreateNow();
            //new Consumer().Consume();
            Console.ReadKey();
        }
    }
}
