using System;
using YBNpgsql;
namespace Yugabyte_CSharp_Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            var connStringBuilder = "host=127.0.0.1,127.0.0.2,127.0.0.3;port=5433;database=yugabyte;userid=yugabyte;ApplicationName=tstest;Load Balance Hosts=true;Options=-c yb_read_from_followers=true;Topology Keys=onprem.pdc.rack1:1,onprem.fdc.rack1:2,azure.us-south-central.zone1:3";
            NpgsqlConnection connection = new NpgsqlConnection(connStringBuilder);
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

                //NpgsqlTransaction transaction = connection.BeginTransaction();
                //SET TRANSACTION READ ONLY;  // https://github.com/npgsql/npgsql/issues/867#issuecomment-157060944

                NpgsqlCommand sessionCharacterisitcs = new NpgsqlCommand("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", connection);
                sessionCharacterisitcs.ExecuteNonQuery();

                NpgsqlCommand myQuery = new NpgsqlCommand("SELECT name, age, language FROM employee WHERE id = @EmployeeId", connection);
                myQuery.Parameters.Add("@EmployeeId", YBNpgsqlTypes.NpgsqlDbType.Integer);
                myQuery.Parameters["@EmployeeId"].Value = 1;

                while (true)
                {


                    DateTime before = DateTime.Now;
                    NpgsqlDataReader reader = myQuery.ExecuteReader();
                    //transaction.Commit();
                    DateTime after = DateTime.Now;
                    TimeSpan timeSpan = after - before;

                    Console.WriteLine("Query took {0}ms and returned:\nName\tAge\tLanguage", timeSpan.TotalMilliseconds);
                    while (reader.Read())
                    {
                        Console.WriteLine("{0}\t{1}\t{2}", reader.GetString(0), reader.GetInt32(1), reader.GetString(2));
                    }
                    reader.Close();

                    System.Threading.Thread.Sleep(1000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failure: " + ex.Message);
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