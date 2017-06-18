using System;
using System.Threading;
using StackExchange.Redis;

namespace NewMessageQueueTest.Redis
{
    public class RedisServer
    {
        /// <summary>
        /// 数据库对象
        /// </summary>
        private readonly IDatabase _database = ConnectionMultiplexer.Connect(Program.ConnectionString).GetDatabase();

        /// <summary>
        /// 获取接收到消息的次数
        /// </summary>
        private ulong _messageTimes;

        /// <summary>
        /// 多线程所对象
        /// </summary>
        private readonly object _locker = new object();

        /// <summary>
        /// 获取接收到消息的次数
        /// </summary>
        public ulong GetMessageTimes
        {
            get
            {
                lock (_locker)
                {
                    return _messageTimes;
                }
            }
            private set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(value));
                lock (_locker)
                {
                    _messageTimes = value;
                }
            }

        }

        /// <summary>
        /// 单例对象，用于获取当前对象的属性
        /// </summary>
        public static RedisServer Instance { get; } = new RedisServer();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private RedisServer() { }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <param name="callback">获取到一条数据的回调函数</param>
        public async void GetMessage(Action<RedisValue> callback)
        {
            Console.WriteLine("开始监听队列");
            while (true)//简单的监听loop
            {
                if (_database.ListLength("TestQueue") == 0)//没有数据就等待一次
                    continue;
                while (_database.ListLength("TestQueue") > 0)//有数据全部接收
                {
                    GetMessageTimes++;
                    callback(await _database.ListRightPopAsync("TestQueue"));
                }
                Thread.Sleep(500);
            }
        }
    }
}
