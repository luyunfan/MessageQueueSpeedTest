using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using KafkaNet;
using KafkaNet.Protocol;

namespace NewMessageQueueTest.Kafka
{
    public class KafkaClient : IMessageClient
    {
        /// <summary>
        /// 发送消息的次数
        /// </summary>
        public ulong SendTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取客户端对象
        /// </summary>
        public static KafkaClient Instance { get; } = new KafkaClient();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private KafkaClient() { }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="times">发送次数</param>
        /// <param name="size">消息大小（B）</param>
        public void SendMessages(ulong times, uint size)
        {
            var router = KafkaConnectionManager.Manager.Router;
            var client = new Producer(router);
            var messages = new Message[times];
            for (ulong i = 0; i < times; i++)
            {
                SendTimes++;
                messages[i] = new Message(Encoding.Default.GetString(new byte[size]));
            }
            using (client)
            {
                client.SendMessageAsync("TestQueue", messages).Wait();
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
