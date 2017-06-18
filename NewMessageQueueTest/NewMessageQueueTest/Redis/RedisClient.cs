using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;

namespace NewMessageQueueTest.Redis
{
    public class RedisClient : IMessageClient
    {
        /// <summary>
        /// 数据库对象
        /// </summary>
        private readonly IDatabase _database = ConnectionMultiplexer.Connect(Program.ConnectionString).GetDatabase();

        /// <summary>
        /// 发送消息的次数
        /// </summary>
        public ulong SendTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取客户端对象
        /// </summary>
        public static RedisClient Instance { get; } = new RedisClient();

        /// <summary>
        /// 多线程锁对象
        /// </summary>
        private readonly object _locker = new object();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private RedisClient() { }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="times">发送次数</param>
        /// <param name="size">消息大小（B）</param>
        public void SendMessages(ulong times, uint size)
        {
            for (ulong i = 0; i < times; i++)
            {
                lock (_locker) //自增考虑线程安全
                {
                    SendTimes++;
                }
                _database.ListLeftPush("TestQueue", new byte[size]);
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
