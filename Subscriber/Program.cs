using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Serilog;
using System;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using MQTTnet.Server;

namespace Subscriber
{
    class Subscriber
    {
        private static int MessageCounter = 0;

        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.FromLogContext()
            .WriteTo.Console()
            .CreateLogger();

            var mqttFactory = new MqttFactory();
            IMqttClient client = mqttFactory.CreateMqttClient();
            var options = new MqttClientOptionsBuilder()
                            //.WithClientId("tbig-f43ac5a8")
                            //.WithTcpServer("test.mosquitto.org", 1883)
                            //.WithCredentials(username: "wildcard")
                            //.WithCleanSession()
                            //.Build();
                            //.WithTcpServer("bqtwe.crystalmq.com", 1883)
                            //.WithCredentials(username: "highlysecure", password: "N4xnpPTru43T8Lmk")
                            //.WithCleanSession()
                            //.Build();
                            .WithTcpServer("localhost", 1883)
                            .WithCleanSession()
                            .Build();

            client.UseConnectedHandler(async e =>
            {   
                Console.WriteLine("Successfully connected.");
                var topicFilter = new MqttTopicFilterBuilder()
                                       //.WithTopic("tbg/#")
                                       //.WithTopic("#")
                                       .WithTopic("test/publish")
                                       .Build();
                await client.SubscribeAsync(topicFilter);


            });

            client.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Successfully disconnected.");
            });

            client.UseApplicationMessageReceivedHandler(e =>
            {

                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                //Console.WriteLine(payload);

                MessageCounter++;

                Log.Logger.Information(
                    "MessageId: {MessageCounter} - TimeStamp: {TimeStamp} - Payload = {payload}",
                    MessageCounter,
                    DateTime.Now,
                    payload
                );

                //string jsonData = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                //dynamic data = JObject.Parse(jsonData);

                //SqlConnection sqlConnection;
                //string connectionString = @"Data Source=localhost;
                //                            Initial Catalog=TBGSiteData;
                //                            Integrated Security=True;
                //                            Connect Timeout=30;
                //                            Encrypt=False;
                //                            TrustServerCertificate=False;
                //                            ApplicationIntent=ReadWrite;
                //                            MultiSubnetFailover=False";
                //try
                //{
                //    sqlConnection = new SqlConnection(connectionString);
                //    sqlConnection.Open();
                //    string insertQuery = "INSERT INTO trxASMIOTLog(Device, Pargroup, Parcode, Value, TimeOccurred) VALUES('" + data.Device + "','" + data.Pargroup + "','" + data.Parcode + "','" + data.Value + "', '" + data.TimeOccurred + "')";
                //    SqlCommand insertCommand = new SqlCommand(insertQuery, sqlConnection);
                //    insertCommand.ExecuteNonQuery();
                //    sqlConnection.Close();

                //}
                //catch (Exception ex)
                //{
                //    Console.WriteLine(ex.Message);
                //}

            });

            await client.ConnectAsync(options);

            Console.ReadLine();

            await client.DisconnectAsync();


        }
    }
}
