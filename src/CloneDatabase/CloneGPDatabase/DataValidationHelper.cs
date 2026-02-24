using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CloneGPDatabase
{
    internal class DataValidationHelper
    {
        public static async Task<int> ValidateRowCounts(SqlConnection sourceConn, SqlConnection destinationConn)
        {
            // Collect the full table list from the source first, then close the reader
            // before issuing per-table COUNT queries to avoid multiple active result sets.
            var tables = new List<(string Schema, string TableName)>();

            using (var cmd = new SqlCommand(@"
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME", sourceConn))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    tables.Add((reader.GetString(0), reader.GetString(1)));
            }

            Logger.Log($"-- Validating row counts for {tables.Count} tables");

            int mismatchCount = 0;

            foreach (var table in tables)
            {
                long sourceCount = await GetRowCount(sourceConn, table.Schema, table.TableName);
                long destCount = await GetRowCount(destinationConn, table.Schema, table.TableName);

                if (sourceCount != destCount)
                {
                    string destDisplay = destCount == -1 ? "table not found" : destCount.ToString();
                    Logger.Log($"   MISMATCH [{table.Schema}].[{table.TableName}]: Source={sourceCount}, Destination={destDisplay}");
                    mismatchCount++;
                }
            }

            return mismatchCount;
        }

        private static async Task<long> GetRowCount(SqlConnection conn, string schema, string tableName)
        {
            try
            {
                using (var cmd = new SqlCommand($"SELECT COUNT_BIG(*) FROM [{schema}].[{tableName}]", conn))
                    return (long)await cmd.ExecuteScalarAsync();
            }
            catch (SqlException)
            {
                return -1;
            }
        }
    }
}
