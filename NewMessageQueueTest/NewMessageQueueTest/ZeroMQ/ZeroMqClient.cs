using System;
using System.Collections.Generic;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;

namespace NewMessageQueueTest.ZeroMQ
{
    public class ZeroMqClient : IMessageClient
    {
        /// <summary>
        /// 连接本机的默认连接字符串
        /// </summary>
        private readonly string _defaultConnectionString = Program.ConnectionString;

        /// <summary>
        /// 发送消息的次数
        /// </summary>
        public ulong SendTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取客户端对象
        /// </summary>
        public static ZeroMqClient Instance { get; } = new ZeroMqClient();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private ZeroMqClient() { }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="times">发送次数</param>
        /// <param name="size">消息大小（B）</param>
        public void SendMessages(ulong times, uint size)
        {
            var sender = new DealerSocket($"tcp://{_defaultConnectionString}:5557");//发送消息
            for (ulong i = 0; i < times; i++)
            {
                SendTimes++;
                sender.SendFrame(new byte[size]);
            }
        }

        /// <summary>
        /// 多线程向消息队列发送一系列数据
        /// </summary>
        /// <param name="times">每条线程发送消息条数</param>
        /// <param name="size">每条消息的大小</param>
        /// <param name="threads">多少个线程来发送</param>
        public void SendMessages(ulong times, uint size, uint threads)
        {
            if (threads == 1)
                SendMessages(times, size);
            else
            {
                var threadList = new List<Thread>((int)threads);//存储线程的列表
                for (var i = 0; i < threads; i++)
                {
                    var item = new Thread(() => SendMessages(times, size));//每个线程发送一条
                    threadList.Add(item);
                    item.Start();
                }
                threadList.ForEach(item => item.Join());//阻塞等待所有线程发送完毕
            }
            Console.WriteLine("数据发送完成！");
            Console.WriteLine("按任意键退出程序....");
        }
    }

}
