using System;
using YBNpgsql;
namespace Yugabyte_CSharp_Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            var connStringBuilder = "host=127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5;port=5433;database=yugabyte;userid=yugabyte;ApplicationName=tstest;Pooling=true;Load Balance Hosts=true;Options=-c yb_read_from_followers=true;Topology Keys=onprem.pdc.rack1:1,onprem.pdc.rack2:2,onprem.fdc.rack1:3,onprem.fdc.rack2:4,azure.us-south-central.zone1:5";
            NpgsqlConnection connection = new NpgsqlConnection(connStringBuilder);
            try
            {
                while (true)
                {
                    try
                    {
                        connection.Open();

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
                        myQuery.Parameters.Add("@EmployeeId", YBNpgsqlTypes.NpgsqlDbType.Integer);  // YBNpgsqlTypes
                        myQuery.Parameters["@EmployeeId"].Value = 1;

                        while (true)
                        {
                            DateTime before = DateTime.Now;
                            NpgsqlDataReader reader = myQuery.ExecuteReader();
                            DateTime after = DateTime.Now;
                            TimeSpan timeSpan = after - before;

                            Console.WriteLine("Query to {0} took {1}ms and returned:\nName\tAge\tLanguage", ip, timeSpan.TotalMilliseconds);
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
                        Console.WriteLine("Inner catch: " + ex.Message);
                    }
                    finally
                    {
                        connection.Close();
                    }
                } /*while*/
            }
            catch (Exception ex)
            {
                Console.WriteLine("Outer catch: " + ex.Message);
            }
            finally
            {
                if (connection.State != System.Data.ConnectionState.Closed)
                {
                    connection.Close();
                }
            }
        }
    }
}