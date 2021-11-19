using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQExample
{
    class Program
    {

        static void Main(string[] args)
        {
            //Bağlantı Açıldı
            RabbitMqHelper.CloseOrOpenConnection();
            //Channel Açıldı
            RabbitMqHelper.CreateChannel();
            //Exchange Oluşturuldu
            RabbitMqHelper.CreateExchange("MyFirstExchange", ExchangeType.Direct);
            //Queque Oluşturuldu
            RabbitMqHelper.CreateQueue("MyFirstQueque");
            //Exchange Ve Queue key ile bağlandı
            RabbitMqHelper.BindQueue("MyFirstQueque", "MyFirstExchange", "MyFirstQueque");
            //Publish Data
            for (int i = 0; i < 50000; i++)
            {
                RabbitMqHelper.PublishChannel("MyFirstExchange", "MyFirstQueque", "Gönderilmiş olan Metin-"+i);
            }
            //Consume Data
            for (int i = 0; i < 20; i++)
            {
                RabbitMqHelper.GetData("MyFirstQueque",i);
            }
 




            Console.ReadKey();
        }
    }
    public static class RabbitMqHelper
    {
        private static IConnection connection;
        private static bool isConnecitonOpen;
        private static IModel channel;
        public static void CloseOrOpenConnection()
        {
            if (!isConnecitonOpen || connection == null)
            {
                connection = GetConnectionRabbitMq();
                Console.WriteLine("Bağlantı açıldı");
            }
            else
            {
                connection.Close();
                Console.WriteLine("Bağlantı kapandı");
            }
        }
        private static IConnection GetConnectionRabbitMq()
        {
            var rabbitMqConnectionString = "amqp://guest:guest@localhost:5672";
            var factory = new ConnectionFactory
            {
                Uri = new Uri(rabbitMqConnectionString, UriKind.RelativeOrAbsolute),
                DispatchConsumersAsync = true
            };
            return factory.CreateConnection();
        }
        public static void CreateExchange(string exchangeName, string exchangeType)
        {
            //Direct exchange =Router Key'e göre  Queue'yu bulur
            //Topic exchange= belli patterne'e göre
            //Örnek-1 isim.* burada yıldız yanlızca ilk noktaya kadar bakar 
            //Örnek-2 isim.# burada birden fazla . olup olmadığına bakmaz herşeye okey der
            //Header exchange=Headerlara göre Queue'ya gönderir any yada all
            //Deadletter exchange=Herhangi bi Queue bulmazsa işlem görür
            channel.ExchangeDeclare(exchangeName, exchangeType);
            Console.WriteLine("Exchange oluşturuldu");
        }
        public static void CreateChannel()
        {
            Console.WriteLine("Channel açıldı");
            channel = connection.CreateModel();
        }
        public static void CreateQueue(string queue)
        {
            channel.QueueDeclare(queue, 
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
        }
        public static void BindQueue(string queue, string exchange, string routingKey)
        {
            channel.QueueBind(queue, exchange, routingKey);
        }
        public static void PublishChannel(string exchange, string routingKey, object data)
        {
            var dataConversion = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
            channel.BasicPublish(exchange, routingKey, null, dataConversion);
            Console.WriteLine("Veri Publish Edildi");
        }

        public static Task GetData(string queue,int consumerId)
        {
            return Task.Run(() => {
                var consumerEvent = new AsyncEventingBasicConsumer(channel);
                consumerEvent.Received += async (ch, e) =>
                {
                    var byteArr = e.Body.ToArray();
                    var bodyStr = Encoding.UTF8.GetString(byteArr);
                    Console.WriteLine(consumerId + "  " + bodyStr.ToString());
                    await Task.Yield();
                };
                channel.BasicConsume(queue, true, consumerEvent);
            });

    
        }
    }





}
