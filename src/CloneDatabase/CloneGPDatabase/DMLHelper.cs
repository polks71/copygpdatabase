using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace CloneGPDatabase
{
    internal class DMLHelper
    {
        public static List<string> GetTables(SqlConnection sourceConn)
        {
            var tables = new List<string>();
            var getTablesCmd = new SqlCommand(@"
                SELECT TABLE_NAME, TABLE_SCHEMA
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            ", sourceConn);

            using (var reader = getTablesCmd.ExecuteReader())
            {
                tables.Add($"{reader.GetString(1)}.{reader.GetString(0)}");
            }
            return tables;
        }
        public static async Task<List<string>> DropAndRecreateTables(SqlConnection destinationConn, SqlConnection sourceConn, bool skipIfExists = false)
        {
            var tables = new List<string>();
            ServerConnection serverConnection = new ServerConnection(sourceConn);
            Server server = new Server(serverConnection);

            // Reference the database.    
            Database db = server.Databases[sourceConn.Database];

            Scripter createScripter = new Scripter(server);
            createScripter.Options.ScriptDrops = false;
            createScripter.Options.NoCollation = true;
            createScripter.Options.ScriptSchema = true; // To include schema
            createScripter.Options.SchemaQualify = true;
            createScripter.Options.WithDependencies = false;
            createScripter.Options.Indexes = false;   // To include indexes  

            Scripter dropScripter = new Scripter(server);
            dropScripter.Options.ScriptDrops = true;
            

            var sb = new StringBuilder();
            int i = 0;
            foreach (Table tb in db.Tables)
            {
                // check if the table is not a system table  
                if (tb.IsSystemObject == false)
                {
                    if (skipIfExists && TableExistsInDestination(destinationConn, tb.Schema, tb.Name))
                    {
                        Console.WriteLine($"-- Skipping {tb.Name} (already exists in destination)");
                        i++;
                        continue;
                    }

                    Console.WriteLine("-- Scripting for table " + tb.Name);
                    tables.Add(tb.Name);
                    sb.AppendLine($"----- START {tb.Name} -----");

                    System.Collections.Specialized.StringCollection dropSc = dropScripter.Script(new Urn[] { tb.Urn });
                    foreach (string st in dropSc)
                    {
                        //Console.WriteLine(st);
                        sb.AppendLine(st);
                    }
                    //Console.WriteLine("--");

                    // Generating script for table tb  
                    System.Collections.Specialized.StringCollection sc = createScripter.Script(new Urn[] { tb.Urn });
                    foreach (string st in sc)
                    {
                        //Console.WriteLine(st);
                        sb.AppendLine(st);
                    }
                    //Console.WriteLine("--");
                    sb.AppendLine($"----- END {tb.Name} -----");

                    var sqlcommand = new SqlCommand(sb.ToString(), destinationConn);
                    await sqlcommand.ExecuteNonQueryAsync();
                    sb.Clear();

                }
                i++;
                //if (i > 5) break;
            }
            return tables;
        }

        private static bool TableExistsInDestination(SqlConnection conn, string schema, string tableName)
        {
            using (var cmd = new SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema AND TABLE_NAME = @name",
                conn))
            {
                cmd.Parameters.AddWithValue("@schema", schema);
                cmd.Parameters.AddWithValue("@name", tableName);
                return (int)cmd.ExecuteScalar() > 0;
            }
        }
    }
}
