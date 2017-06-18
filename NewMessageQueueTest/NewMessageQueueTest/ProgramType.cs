namespace NewMessageQueueTest
{
    /// <summary>
    /// 程序运行的类型 服务器/客户端/错误
    /// </summary>
    internal enum ProgramType
    {
        Redis, RabbitMq, ZeroMq, Kafka,
        Server, Client, Error
    }
}
