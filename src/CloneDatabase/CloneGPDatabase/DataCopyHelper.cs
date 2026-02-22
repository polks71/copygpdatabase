using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            public bool HasIdentity { get; set; }
        }

        public static async Task<int> CopyData(SqlConnection sourceConn, SqlConnection destinationConn)
        {
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
                foreach (Index idx in tb.Indexes)
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
                    Console.WriteLine($"-- Skipping {tb.Schema}.{tb.Name} (no primary key)");
                    continue;
                }

                var columnNames = new List<string>();
                bool hasIdentity = false;
                foreach (Column col in tb.Columns)
                {
                    if (!col.Computed)
                    {
                        columnNames.Add(col.Name);
                        if (col.Identity) hasIdentity = true;
                    }
                }

                tableInfos.Add(new TableCopyInfo
                {
                    Schema = tb.Schema,
                    TableName = tb.Name,
                    ColumnNames = columnNames,
                    PkColumnNames = pkColumns,
                    HasIdentity = hasIdentity
                });
            }

            int totalRows = 0;
            foreach (var tableInfo in tableInfos)
            {
                int rowCount = await CopyTableData(sourceConn, destinationConn, tableInfo);
                Console.WriteLine($"   Copied {rowCount} rows into [{tableInfo.Schema}].[{tableInfo.TableName}]");
                totalRows += rowCount;
            }

            return totalRows;
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
            // Example generated SQL for a table with an identity PK and one non-PK column:
            //
            //   SET IDENTITY_INSERT [dbo].[MyTable] ON;
            //   MERGE INTO [dbo].[MyTable] AS target
            //   USING (VALUES (@p0, @p1)) AS source ([Id], [Name])
            //   ON (target.[Id] = source.[Id])
            //   WHEN MATCHED THEN UPDATE SET target.[Name] = source.[Name]
            //   WHEN NOT MATCHED THEN INSERT ([Id], [Name]) VALUES (source.[Id], source.[Name]);
            //   SET IDENTITY_INSERT [dbo].[MyTable] OFF;
            var sb = new StringBuilder();

            if (tableInfo.HasIdentity)
                sb.AppendLine($"SET IDENTITY_INSERT [{tableInfo.Schema}].[{tableInfo.TableName}] ON;");

            sb.AppendLine($"MERGE INTO [{tableInfo.Schema}].[{tableInfo.TableName}] AS target");
            sb.AppendLine($"USING (VALUES ({string.Join(", ", paramNames)})) AS source ({insertCols})");
            sb.AppendLine($"ON ({onClause})");

            if (nonPkColumns.Count > 0)
            {
                var updateSet = string.Join(", ", nonPkColumns.Select(c => $"target.[{c}] = source.[{c}]"));
                sb.AppendLine($"WHEN MATCHED THEN UPDATE SET {updateSet}");
            }

            sb.AppendLine($"WHEN NOT MATCHED THEN INSERT ({insertCols}) VALUES ({insertVals});");

            if (tableInfo.HasIdentity)
                sb.AppendLine($"SET IDENTITY_INSERT [{tableInfo.Schema}].[{tableInfo.TableName}] OFF;");

            string mergeTemplate = sb.ToString();

            var selectSql = $"SELECT {insertCols} FROM [{tableInfo.Schema}].[{tableInfo.TableName}]";
            int rowCount = 0;

            Console.WriteLine($"-- Copying data for table [{tableInfo.Schema}].[{tableInfo.TableName}]");

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
                        await destCmd.ExecuteNonQueryAsync();
                        rowCount++;
                    }
                }
            }

            return rowCount;
        }
    }
}
