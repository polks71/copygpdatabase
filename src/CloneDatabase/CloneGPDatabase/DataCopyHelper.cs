using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CloneGPDatabase
{
    internal class DataCopyHelper
    {
        private class TableCopyInfo
        {
            public string Schema { get; set; }
            public string TableName { get; set; }
            public List<string> ColumnNames { get; set; }
            public List<string> PkColumnNames { get; set; }
        }

        public static async Task<int> CopyData(SqlConnection sourceConn, SqlConnection destinationConn, int maxThreads = 4)
        {
            // Extract connection strings so each task can open its own independent connections.
            string sourceConnectionString = sourceConn.ConnectionString;
            string destinationConnectionString = destinationConn.ConnectionString;

            // Collect all table metadata via SMO before opening any data readers
            // to avoid multiple active result sets on the same connection.
            var tableInfos = new List<TableCopyInfo>();

            ServerConnection serverConnection = new ServerConnection(sourceConn);
            Server server = new Server(serverConnection);
            Database db = server.Databases[sourceConn.Database];

            foreach (Table tb in db.Tables)
            {
                if (tb.IsSystemObject) continue;

                var pkColumns = new List<string>();
                foreach (Microsoft.SqlServer.Management.Smo.Index idx in tb.Indexes)
                {
                    if (idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
                    {
                        foreach (IndexedColumn col in idx.IndexedColumns)
                            pkColumns.Add(col.Name);
                        break;
                    }
                }

                if (pkColumns.Count == 0)
                {
                    Logger.Log($"-- Skipping {tb.Schema}.{tb.Name} (no primary key)");
                    continue;
                }

                var columnNames = new List<string>();
                foreach (Column col in tb.Columns)
                {
                    if (!col.Computed)
                        columnNames.Add(col.Name);
                }

                tableInfos.Add(new TableCopyInfo
                {
                    Schema = tb.Schema,
                    TableName = tb.Name,
                    ColumnNames = columnNames,
                    PkColumnNames = pkColumns
                });
            }

            var semaphore = new SemaphoreSlim(maxThreads);
            var tasks = new List<Task<int>>();

            foreach (var tableInfo in tableInfos)
            {
                // Block (asynchronously) until a thread slot is available.
                await semaphore.WaitAsync();

                var info = tableInfo;
                var task = Task.Run(async () =>
                {
                    try
                    {
                        using (var srcConn = new SqlConnection(sourceConnectionString))
                        using (var dstConn = new SqlConnection(destinationConnectionString))
                        {
                            await srcConn.OpenAsync();
                            await dstConn.OpenAsync();

                            int count = await CopyTableData(srcConn, dstConn, info);
                            Logger.Log($"   Copied {count} rows into [{info.Schema}].[{info.TableName}]");
                            return count;
                        }
                    }
                    finally
                    {
                        // Releasing here lets the next table start immediately.
                        semaphore.Release();
                    }
                });

                tasks.Add(task);
            }

            int[] results = await Task.WhenAll(tasks);
            return results.Sum();
        }

        private static async Task<int> CopyTableData(SqlConnection sourceConn, SqlConnection destinationConn, TableCopyInfo tableInfo)
        {
            var columnNames = tableInfo.ColumnNames;
            var pkColumns = tableInfo.PkColumnNames;
            var nonPkColumns = columnNames.Where(c => !pkColumns.Contains(c)).ToList();

            var quotedColumns = columnNames.Select(c => $"[{c}]").ToList();
            var paramNames = columnNames.Select((c, i) => $"@p{i}").ToList();

            var onClause = string.Join(" AND ", pkColumns.Select(pk => $"target.[{pk}] = source.[{pk}]"));
            var insertCols = string.Join(", ", quotedColumns);
            var insertVals = string.Join(", ", columnNames.Select(c => $"source.[{c}]"));

            // Build the MERGE template once; parameters are rebound per row.
            // Example generated SQL for a table with a PK and one non-PK column:
            //
            //   MERGE INTO [dbo].[MyTable] AS target
            //   USING (VALUES (@p0, @p1)) AS source ([Id], [Name])
            //   ON (target.[Id] = source.[Id])
            //   WHEN MATCHED THEN UPDATE SET target.[Name] = source.[Name]
            //   WHEN NOT MATCHED THEN INSERT ([Id], [Name]) VALUES (source.[Id], source.[Name]);
            var sb = new StringBuilder();

            sb.AppendLine($"MERGE INTO [{tableInfo.Schema}].[{tableInfo.TableName}] AS target");
            sb.AppendLine($"USING (VALUES ({string.Join(", ", paramNames)})) AS source ({insertCols})");
            sb.AppendLine($"ON ({onClause})");

            if (nonPkColumns.Count > 0)
            {
                var updateSet = string.Join(", ", nonPkColumns.Select(c => $"target.[{c}] = source.[{c}]"));
                sb.AppendLine($"WHEN MATCHED THEN UPDATE SET {updateSet}");
            }

            sb.AppendLine($"WHEN NOT MATCHED THEN INSERT ({insertCols}) VALUES ({insertVals});");

            string mergeTemplate = sb.ToString();

            var selectSql = $"SELECT {insertCols} FROM [{tableInfo.Schema}].[{tableInfo.TableName}]";
            int rowCount = 0;

            Logger.Log($"-- Copying data for table [{tableInfo.Schema}].[{tableInfo.TableName}]");

            using (var sourceCmd = new SqlCommand(selectSql, sourceConn))
            using (var reader = await sourceCmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    using (var destCmd = new SqlCommand(mergeTemplate, destinationConn))
                    {
                        for (int i = 0; i < columnNames.Count; i++)
                        {
                            var value = reader.IsDBNull(i) ? (object)DBNull.Value : reader.GetValue(i);
                            destCmd.Parameters.AddWithValue($"@p{i}", value);
                        }
                        destCmd.CommandTimeout = 0; // In case of very large tables, we don't want to timeout.
                        await destCmd.ExecuteNonQueryAsync();
                        rowCount++;
                    }
                }
            }

            return rowCount;
        }
    }
}
