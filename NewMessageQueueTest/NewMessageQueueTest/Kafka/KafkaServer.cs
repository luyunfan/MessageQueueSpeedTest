using System;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace NewMessageQueueTest.Kafka
{
    public class KafkaServer
    {
        /// <summary>
        /// 获取接收到消息的次数
        /// </summary>
        public ulong GetMessageTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取Server对象
        /// </summary>
        public static KafkaServer Instance { get; } = new KafkaServer();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private KafkaServer() { }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <param name="callback">获取到一条数据的回调函数</param>
        public void GetMessage(Action<Message> callback)
        {
            var router = KafkaConnectionManager.Manager.Router;
            var consumer = new Consumer(new ConsumerOptions("TestQueue", router));
            foreach (var message in consumer.Consume())
            {
                GetMessageTimes++;
                callback(message);
            }
        }
    }
}
