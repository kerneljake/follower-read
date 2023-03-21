using System;
using System.Data.Common;
using Npgsql;
namespace Yugabyte_CSharp_Demo
{
    class Program
    {
        private static NpgsqlDataSource? dataSource = null;

        static void Main(string[] args)
        {
            //var connectionString = "host=127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5;port=5433;database=yugabyte;userid=yugabyte;ApplicationName=tstest;YB Servers Refresh Interval=10;Pooling=true;Load Balance Hosts=true;Options=-c yb_read_from_followers=true;Topology Keys=onprem.pdc.*:1,onprem.fdc.*:2,azure.us-south-central.zone1:3"; // YBNpgsql
            var connectionString = "host=127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5;port=5433;database=yugabyte;userid=yugabyte;ApplicationName=tstest;Pooling=true;Load Balance Hosts=true"; // Npgsql
            var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
            dataSource = dataSourceBuilder.Build();

            Random random = new Random();

            for (int i = 0; i < 5; i++)
            {
                Thread t = new Thread(Worker);
                t.Start();
                System.Threading.Thread.Sleep(random.Next(800,1000));
                //t.Join(); // blocks caller
            }
            Console.WriteLine("Done");
        }

        static void Worker() {

            Thread thread = Thread.CurrentThread;

            /* connect to the database */
            while (true)
            {
                NpgsqlConnection connection = dataSource!.OpenConnection(); // null forgiving operator !

                try
                {
                    /*  
                    NpgsqlCommand empCreateCmd = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS employee (id int PRIMARY KEY, name varchar, age int, language varchar) SPLIT INTO 1 TABLETS;", connection);
                    empCreateCmd.ExecuteNonQuery();
                    Console.WriteLine("Created table Employee");

                    NpgsqlCommand empInsertCmd = new NpgsqlCommand("INSERT INTO employee (id, name, age, language) VALUES (1, 'John', 35, 'CSharp');", connection);
                    int numRows = empInsertCmd.ExecuteNonQuery();
                    Console.WriteLine("Inserted data (1, 'John', 35, 'CSharp')");
                    */

                    NpgsqlCommand myCommand = new NpgsqlCommand("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", connection);
                    myCommand.ExecuteNonQuery();

                    myCommand.CommandText = "SELECT(host(inet_server_addr()))";
                    NpgsqlDataReader myReader = myCommand.ExecuteReader();
                    myReader.Read();
                    String ip = myReader.GetString(0);
                    myReader.Close();

                    NpgsqlCommand myQuery = new NpgsqlCommand("SELECT name, age, language FROM employee WHERE id = @EmployeeId", connection);
                    myQuery.Parameters.Add("@EmployeeId", NpgsqlTypes.NpgsqlDbType.Integer);  // YBNpgsqlTypes
                    myQuery.Parameters["@EmployeeId"].Value = 1;

                    /* read queries continuously */
                    while (true)
                    {
                        DateTime before = DateTime.Now;
                        NpgsqlDataReader reader = myQuery.ExecuteReader();
                        DateTime after = DateTime.Now;
                        TimeSpan timeSpan = after - before;

                        Console.WriteLine("Thread {0,2} Query to {1} took {2}ms and returned:\nName\tAge\tLanguage", thread.ManagedThreadId, ip, timeSpan.TotalMilliseconds);
                        while (reader.Read())
                        {
                            Console.WriteLine("{0}\t{1}\t{2}", reader.GetString(0), reader.GetInt32(1), reader.GetString(2));
                        }
                        reader.Close();

                        System.Threading.Thread.Sleep(1000);
                    } /*while*/
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Inner catch (Thread {0,2}): " + ex.Message, thread.ManagedThreadId);
                }
                finally
                {
                    connection.Close();
                }
            } /*while*/

        } /*worker*/

    } /*class*/

} /*namespace*/