// See https://aka.ms/new-console-template for more information
using CloneGPDatabase;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

internal class Program
{
    static async Task Main(string[] args)
    {
        IConfigurationRoot config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json")
    .Build();

        Logger.Initialize(config[CloneConstants.LogFilePathKey] ?? "clone_log.txt");

        Logger.Log("Welcome to the GPClone Utility");

        // This is a simple program to clone a GP database
        // It will prompt the user for the source and destination database connection strings
        // Assume the destination database already exists
        // then copy the data structure of the GP Database to the destination database
        // then copy the data from the source database to the destination database


        string sourceConnectionString = config.GetConnectionString(CloneConstants.SourceSqlConnectionStringKey);
        if (string.IsNullOrWhiteSpace(sourceConnectionString))
        {
            Logger.Log("Source connection string cannot be empty. Update the App Settings file.");
            return;
        }
        Logger.Log("Using configured source connection string");

        string destinationConnectionString = config.GetConnectionString(CloneConstants.DestinationSqlConnectionStringKey);
        if (string.IsNullOrWhiteSpace(destinationConnectionString))
        {
            Logger.Log("Destination connection string cannot be empty. Update the App Settings file.");
            return;
        }

        using (SqlConnection sourceConn = new SqlConnection(sourceConnectionString))
        using (SqlConnection destConn = new SqlConnection(destinationConnectionString))
        {
            sourceConn.Open();
            destConn.Open();

            var tables = await DMLHelper.CreateTablesIfTheyDontExist(destConn, sourceConn);
            Logger.Log($"Created {tables.Count} Tables");

            int maxCopyThreads = int.Parse(config[CloneConstants.MaxCopyThreadsKey] ?? "4");
            var copiedRows = await DataCopyHelper.CopyData(sourceConn, destConn, tables, maxCopyThreads);
            Logger.Log($"Copied {copiedRows} Total Rows");

            int mismatches = await DataValidationHelper.ValidateRowCounts(sourceConn, destConn, tables);
            Logger.Log(mismatches == 0
                ? "Validation passed: all table row counts match."
                : $"Validation complete: {mismatches} table(s) with mismatched row counts.");
        }

        Logger.Log("Done");
        Console.ReadLine();
    }
}
