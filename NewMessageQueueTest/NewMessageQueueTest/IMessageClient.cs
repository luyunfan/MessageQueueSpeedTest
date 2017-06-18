namespace NewMessageQueueTest
{
    /// <summary>
    /// 代表不同消息队列的客户端
    /// </summary>
    public interface IMessageClient
    {
        /// <summary>
        /// 向消息队列发送一系列数据
        /// </summary>
        /// <param name="times">发送消息条数</param>
        /// <param name="size">每条消息的大小</param>
        void SendMessages(ulong times, uint size);

        /// <summary>
        /// 多线程向消息队列发送一系列数据
        /// </summary>
        /// <param name="times">每条线程发送消息条数</param>
        /// <param name="size">每条消息的大小</param>
        /// <param name="threads">多少个线程来发送</param>
        void SendMessages(ulong times, uint size, uint threads);
    }
}
