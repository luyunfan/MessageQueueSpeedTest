using System;
using NetMQ;
using NetMQ.Sockets;

namespace NewMessageQueueTest.ZeroMQ
{
    public class ZeroMqServer
    {
        private readonly string _defaultConnectionString = Program.ConnectionString;//连接本机的默认连接字符串

        /// <summary>
        /// 获取接收到消息的次数
        /// </summary>
        public ulong GetMessageTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取当前对象的属性
        /// </summary>
        public static ZeroMqServer Instance { get; } = new ZeroMqServer();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private ZeroMqServer() { }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <param name="callback">获取到一条数据的回调函数</param>
        public void GetMessage(Action<byte[]> callback)
        {
            Console.WriteLine("开始监听队列");
            var receiver = new DealerSocket($"@tcp://{_defaultConnectionString}:5557");//接收消息
            while (true)//简单的监听loop
            {
                var message = receiver.ReceiveFrameBytes();//未接收到消息将阻塞线程
                GetMessageTimes++;
                callback(message);
            }
        }
    }
}
