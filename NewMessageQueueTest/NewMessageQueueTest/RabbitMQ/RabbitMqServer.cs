using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NewMessageQueueTest.RabbitMQ
{
    public class RabbitMqServer
    {
        private readonly IModel _channel = RabbitMqConnectionManager.Manager.Channel;//获得一个连接对象

        /// <summary>
        /// 获取接收到消息的次数
        /// </summary>
        public ulong GetMessageTimes { get; private set; }

        /// <summary>
        /// 单例对象，用于获取Server对象
        /// </summary>
        public static RabbitMqServer Instance { get; } = new RabbitMqServer();

        /// <summary>
        /// 私有构造方法
        /// </summary>
        private RabbitMqServer() { }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <param name="callback">获取到一条数据的回调函数</param>
        public void GetMessage(Action<BasicDeliverEventArgs> callback)
        {
            Console.WriteLine("开始监听队列");
            //每次处理单个Message
            _channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            var eventConsumer = new EventingBasicConsumer(_channel);
            _channel.BasicConsume(queue: "TestQueue", noAck: false, consumer: eventConsumer);//返回通知才删除队列

            eventConsumer.Received += (model, ea) =>
            {
                GetMessageTimes++;
                callback(ea);
                _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);//返回通知删除
            };
        }
    }
}
