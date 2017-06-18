using System;
using System.Text;
using System.Threading;
using KafkaNet.Protocol;
using NewMessageQueueTest.Kafka;
using NewMessageQueueTest.RabbitMQ;
using NewMessageQueueTest.Redis;
using NewMessageQueueTest.ZeroMQ;
using RabbitMQ.Client.Events;
using StackExchange.Redis;

namespace NewMessageQueueTest
{
    public class Program
    {
        /// <summary>
        /// 当前程序类型
        /// </summary>
        private static ProgramType _programType;

        /// <summary>
        /// 当前程序模式
        /// </summary>
        private static ProgramType _programMode;

        /// <summary>
        /// 接收 or 发送多少次消息
        /// </summary>
        private static ulong _howManyTimes;

        /// <summary>
        /// 客户端使用线程数量
        /// </summary>
        private static uint _threadNum = 1;

        /// <summary>
        /// 客户端发送每条消息的大小
        /// </summary>
        private static uint _messageSize;

        /// <summary>
        /// 向其他类暴露的连接字符串（连接地址）
        /// </summary>
        public static string ConnectionString = "localhost";

        public static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8; //防止乱码
            if (!GetProgramTypeByArgs(args))//通过参数获取失败
            {
                while (true)//用户输入类型和模式
                {
                    if (InputProgramType())
                        break;//输入合法
                    Console.Clear();
                    Console.WriteLine("出现错误，请重新输入：");
                }
            }
            switch (_programType)
            {
                case ProgramType.Redis:
                    switch (_programMode)
                    {
                        case ProgramType.Client:
                            DoClientTesk(RedisClient.Instance, showMessage: "作为Redis客户端", times: _howManyTimes, size: _messageSize);
                            break;
                        case ProgramType.Server:
                            DoServerTask<RedisValue>("作为Redis服务端", RedisServer.Instance.GetMessage, () => RedisServer.Instance.GetMessageTimes);
                            break;
                        default:
                            return;
                    }
                    break;
                case ProgramType.RabbitMq:
                    switch (_programMode)
                    {
                        case ProgramType.Client:
                            DoClientTesk(RabbitMqClient.Instance, showMessage: "作为RabbitMQ客户端", times: _howManyTimes, size: _messageSize);
                            break;
                        case ProgramType.Server:
                            DoServerTask<BasicDeliverEventArgs>("作为RabbitMQ服务端", RabbitMqServer.Instance.GetMessage, () => RabbitMqServer.Instance.GetMessageTimes);
                            break;
                        default:
                            return;
                    }
                    break;
                case ProgramType.ZeroMq:
                    switch (_programMode)
                    {
                        case ProgramType.Client:
                            DoClientTesk(ZeroMqClient.Instance, showMessage: "作为ZeroMQ客户端", times: _howManyTimes, size: _messageSize);
                            break;
                        case ProgramType.Server:
                            DoServerTask<byte[]>("作为ZeroMQ服务端", ZeroMqServer.Instance.GetMessage, () => ZeroMqServer.Instance.GetMessageTimes);
                            break;
                        default:
                            return;
                    }
                    break;
                case ProgramType.Kafka:
                    switch (_programMode)
                    {
                        case ProgramType.Client:
                            DoClientTesk(KafkaClient.Instance, showMessage: "作为Kafka客户端", times: _howManyTimes, size: _messageSize);
                            break;
                        case ProgramType.Server:
                            DoServerTask<Message>("作为Kafka服务端", KafkaServer.Instance.GetMessage, () => KafkaServer.Instance.GetMessageTimes);
                            break;
                        default:
                            Console.WriteLine("程序类型有误！按任意键退出....");
                            return;
                    }
                    break;
            }
        }

        /// <summary>
        /// 打印参数使用用法
        /// </summary>
        private static void PrintUsage()
        {
            Console.WriteLine("用法：NewMessageQueueTest <ProgramType> <ProgramMode> <DataNum> [ThreadNum] [MessageSize] [ip]");
            Console.WriteLine("其中ProgramType表示消息队列类型，包括：");
            Console.WriteLine("\t redis、rabbitmq、zeromq、kafka");
            Console.WriteLine("其中ProgramMode表示设置本进程用于发送消息（Client）还是接收消息（Server），包括：");
            Console.WriteLine("\t server、client");
            Console.WriteLine("其中DataNum表示一个线程发送/接收消息数目");
            Console.WriteLine("其中ThreadNumClient表示发送消息开启线程数目");
            Console.WriteLine("其中MessageSize表示每条消息的大小");
            Console.WriteLine("以上DataNum、ThreadNum、MessageSize均为整数，MessageSize单位为字节");
            Console.WriteLine("其中ip表示消息队列目标ip地址");
            Console.WriteLine("\t注：ThreadNum、MessageSize为Client模式必填选项，仅对Client模式有效，ip如果不填则为本机ip");
        }

        /// <summary>
        /// 通过输入参数获取程序类型
        /// </summary>
        /// <param name="inputArgs">输入参数</param>
        /// <returns>是否获取成功</returns>
        private static bool GetProgramTypeByArgs(string[] inputArgs)
        {
            //打印Usage
            if (inputArgs.Length >= 1 && inputArgs[0].ToLower() == "-help")
            {
                PrintUsage();
                Environment.Exit(0);
            }
            //正常参数
            if (inputArgs.Length < 3)//作为Server参数不够
                return false;
            var programType = inputArgs[0].ToLower();//去除大小写干扰
            var runningMode = inputArgs[1].ToLower();
            if (programType != "redis" && programType != "rabbitmq" && programType != "zeromq" && programType != "kafka")
                return false;
            if (runningMode != "server" && runningMode != "client")
                return false;
            if (!ulong.TryParse(inputArgs[2], out _howManyTimes))
                return false;
            _programType = WhatType(programType);
            _programMode = WhatRunningMode(runningMode);
            if (_programMode != ProgramType.Client)//是Server返回true
            {
                if (inputArgs.Length == 4)//检查是否输入了ip
                    ConnectionString = inputArgs[3];
                return true;
            }
            //如果是Client的话：
            if (inputArgs.Length < 5)//作为Client参数不够
                return false;
            if (inputArgs.Length == 6)//检查是否输入了ip
                ConnectionString = inputArgs[5];
            return uint.TryParse(inputArgs[3], out _threadNum) && uint.TryParse(inputArgs[4], out _messageSize);//获取线程数量和单消息大小
        }

        /// <summary>
        /// 让用户自己输入程序运行类型
        /// </summary>
        /// <returns>是否正确输入</returns>
        private static bool InputProgramType()
        {
            Console.WriteLine("输入消息队列地址（不填则默认为本机ip）");
            var inputConnectionString = Console.ReadLine();
            if (inputConnectionString != "")//输入了ip地址
                ConnectionString = inputConnectionString;
            Console.WriteLine("输入程序运行类型 1：Redis 2：RabbitMQ 3：ZeroMQ 4：Kafka");
            var inputType = WhatType(Console.ReadKey());
            Console.WriteLine();
            if (inputType != ProgramType.Error)
                _programType = inputType;
            else
                return false;
            Console.WriteLine("输入程序运行模式 1：Server 2：Client");
            var inputMode = WhatRunningMode(Console.ReadKey());
            Console.WriteLine();
            if (inputMode == ProgramType.Error)
                return false;
            _programMode = inputMode;
            if (inputMode == ProgramType.Server)
            {
                Console.WriteLine("输入接收多少条数据：");
                return ulong.TryParse(Console.ReadLine(), out _howManyTimes);
            }//客户端执行内容：
            Console.WriteLine("输入发送多少条数据：");
            if (!ulong.TryParse(Console.ReadLine(), out _howManyTimes))
                return false;
            Console.WriteLine("输入发送数据线程数：");
            if (!uint.TryParse(Console.ReadLine(), out _threadNum))
                return false;
            Console.WriteLine("输入每条消息的大小（字节）：");
            return uint.TryParse(Console.ReadLine(), out _messageSize);
        }

        /// <summary>
        /// 根据按键值获取程序类型
        /// </summary>
        /// <param name="key">输入的按键</param>
        /// <returns>程序类型</returns>
        private static ProgramType WhatType(ConsoleKeyInfo key)
        {
            switch (key.KeyChar)
            {
                case '1':
                    return ProgramType.Redis;
                case '2':
                    return ProgramType.RabbitMq;
                case '3':
                    return ProgramType.ZeroMq;
                case '4':
                    return ProgramType.Kafka;
                default:
                    return ProgramType.Error;
            }
        }

        /// <summary>
        /// 根据字符串获取程序类型
        /// </summary>
        /// <param name="type">类型字符串</param>
        /// <returns>程序类型</returns>
        private static ProgramType WhatType(string type)
        {
            switch (type)
            {
                case "redis":
                    return ProgramType.Redis;
                case "rabbitmq":
                    return ProgramType.RabbitMq;
                case "zeromq":
                    return ProgramType.ZeroMq;
                case "kafka":
                    return ProgramType.Kafka;
                default:
                    return ProgramType.Error;
            }
        }

        /// <summary>
        /// 根据按键值获取运行模式
        /// </summary>
        /// <param name="key">输入的按键</param>
        /// <returns>运行模式</returns>
        private static ProgramType WhatRunningMode(ConsoleKeyInfo key)
        {
            switch (key.KeyChar)
            {
                case '1':
                    return ProgramType.Server;
                case '2':
                    return ProgramType.Client;
                default:
                    return ProgramType.Error;
            }
        }

        /// <summary>
        /// 根据字符串获取运行模式
        /// </summary>
        /// <param name="mode">输入的字符串</param>
        /// <returns>运行模式</returns>
        private static ProgramType WhatRunningMode(string mode)
        {
            switch (mode)
            {
                case "server":
                    return ProgramType.Server;
                case "client":
                    return ProgramType.Client;
                default:
                    return ProgramType.Error;
            }
        }

        /// <summary>
        /// 执行服务端相关逻辑（接收数据）
        /// </summary>
        /// <typeparam name="T">获取数据方法回调的类型（即服务端收到的Message的类型）</typeparam>
        /// <param name="showMessage">在控制台打印的信息</param>
        /// <param name="getMessage">获取数据方法</param>
        /// <param name="getMessageTimes">获得当前实时消息数的方法</param>
        private static void DoServerTask<T>(string showMessage, Action<Action<T>> getMessage, Func<ulong> getMessageTimes)
        {
            Console.WriteLine(showMessage);
            var timer = new Timer(state => Console.WriteLine("当前接收到数据数目：{0}", getMessageTimes()), state: null, dueTime: 0, period: 3000);//三秒打印一次当前接收到的数据量，Debug用
            GC.KeepAlive(timer);//防止timer被回收
            var isStart = false;
            var isDone = false;
            var stopwatch = new System.Diagnostics.Stopwatch();
            getMessage(value =>
            {
                if (!isStart && !isDone)
                {
                    isStart = true;
                    stopwatch.Start();
                    Console.WriteLine("开始接收数据...");
                }
                if (isDone)
                    return;
                if (getMessageTimes() != _howManyTimes)
                    return;
                stopwatch.Stop();
                isDone = true;
                Console.WriteLine("接收目标数目完成！");
                Console.WriteLine("接收到预设数据运行时间：{0}", stopwatch.Elapsed);
                Console.WriteLine("接收数据条数：{0}", getMessageTimes());
            });
            Console.WriteLine("按任意键退出程序....");
            Console.ReadKey();
        }

        /// <summary>
        /// 执行客户端相关逻辑（发送所有数据）
        /// </summary>
        /// <param name="client">客户端对象</param>
        /// <param name="showMessage">在控制台打印的信息</param>
        /// <param name="times">发送消息的条数</param>
        /// <param name="size">每条消息的大小（B\字节）</param>
        private static void DoClientTesk(IMessageClient client, string showMessage, ulong times, uint size)
        {
            Console.WriteLine(showMessage);
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            client.SendMessages(times, size, _threadNum);//实现时内部应该是多线程异步的，等待所有线程完成后才返回此函数用于计时
            stopwatch.Stop();
            Console.WriteLine("发送数据所用时间：{0}", stopwatch.Elapsed);
            Console.ReadKey();
        }
    }
}
