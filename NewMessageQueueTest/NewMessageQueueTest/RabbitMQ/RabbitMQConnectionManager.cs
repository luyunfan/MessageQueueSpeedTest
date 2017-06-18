using RabbitMQ.Client;

namespace NewMessageQueueTest.RabbitMQ
{
    /// <summary>
    /// 获取RabbitMQ连接对象的单例管理类
    /// </summary>
    public class RabbitMqConnectionManager
    {
        /// <summary>
        /// 连接工厂
        /// </summary>
        private readonly ConnectionFactory _factory = new ConnectionFactory { HostName = Program.ConnectionString, Port = 5672 };

        /// <summary>
        /// 连接对象
        /// </summary>
        private readonly IConnection _connection;

        /// <summary>
        /// 获取Channel对象（channel对象非线程安全，所以每次调用都会返回一个新的）
        /// </summary>
        public IModel Channel
        {
            get
            {
                var channel = _connection.CreateModel();
                channel.QueueDeclare("TestQueue", true, false, false, null);
                return channel;
            }
        }

        /// <summary>
        /// 私有构造方法 初始化连接
        /// </summary>
        private RabbitMqConnectionManager()
        {
            _connection = _factory.CreateConnection();
        }

        /// <summary>
        /// 获取连接管理对象
        /// </summary>
        public static RabbitMqConnectionManager Manager => ManagerCreator.Instance;

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
            internal static readonly RabbitMqConnectionManager Instance = new RabbitMqConnectionManager();
        }
    }
}
