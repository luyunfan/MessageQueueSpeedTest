using System;
using KafkaNet;
using KafkaNet.Model;

namespace NewMessageQueueTest.Kafka
{
    public class KafkaConnectionManager
    {
        /// <summary>
        /// Kafka连接地址
        /// </summary>
        private readonly Uri _serverUri = new Uri($"http://{Program.ConnectionString}:9092");

        /// <summary>
        /// 路由对象
        /// </summary>
        public BrokerRouter Router { get; }

        /// <summary>
        /// 构造函数，用于初始化路由对象
        /// </summary>
        private KafkaConnectionManager()
        {
            var options = new KafkaOptions(_serverUri);
            Router = new BrokerRouter(options);
        }

        /// <summary>
        /// 连接管理对象
        /// </summary>
        public static KafkaConnectionManager Manager => ManagerCreator.Instance;

        /// <summary>
        /// 单例模式的嵌套类，用于实现线程安全的单例模式
        /// </summary>
        internal class ManagerCreator
        {
            /// <summary>
            /// 静态构造方法
            /// </summary>
            static ManagerCreator() { }

            /// <summary>
            /// 获取连接对象
            /// </summary>
            internal static readonly KafkaConnectionManager Instance = new KafkaConnectionManager();
        }
    }
}
