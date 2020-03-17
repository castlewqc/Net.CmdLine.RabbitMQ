using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FanoutCustomer
{
    class Program
    {
        static TopicCustomer customer;
        static void Main(string[] args)
        {
         

            customer = new TopicCustomer();
            customer.Start();
            Exit();
        }
        private static void Exit()
        {
            var value = Console.ReadKey().Key;
            if (value == ConsoleKey.Escape)
            {
                customer.Stop();
                Environment.Exit(Environment.ExitCode);
            }
            else
                Exit();
        }
    }
}
