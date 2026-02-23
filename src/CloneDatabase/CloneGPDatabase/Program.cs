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

        Console.WriteLine("Welcome to the GPClone Utility");

        // This is a simple program to clone a GP database
        // It will prompt the user for the source and destination database connection strings
        // Assume the destination database already exists
        // then copy the data structure of the GP Database to the destination database
        // then copy the data from the source database to the destination database

        Console.WriteLine("Note: This utility will drop each table before creating the table in the destination.");
        Console.WriteLine("Note: Windows authentication is used to connect to the source SQL Server");
        //ask for the connectionstring to the source database


        string sourceConnectionString = config.GetConnectionString(CloneConstants.SourceSqlConnectionStringKey);
        if (string.IsNullOrWhiteSpace(sourceConnectionString))
        {
            Console.WriteLine("Source connection string cannot be empty. Update the App Settings file.");
            return;
        }
        Console.WriteLine("Using configured source connection string");

        string destinationConnectionString = config.GetConnectionString(CloneConstants.DestinationSqlConnectionStringKey);
        if (string.IsNullOrWhiteSpace(destinationConnectionString))
        {
            Console.WriteLine("Destination connection string cannot be empty. Update the App Settings file.");
            return;
        }

        using (SqlConnection sourceConn = new SqlConnection(sourceConnectionString))
        using (SqlConnection destConn = new SqlConnection(destinationConnectionString))
        {
            sourceConn.Open();
            destConn.Open();

            var tables = await DMLHelper.CreateTablesIfTheyDontExist(destConn, sourceConn);
            Console.WriteLine($"Dropped and Created {tables.Count} Tables");

            int maxCopyThreads = int.Parse(config[CloneConstants.MaxCopyThreadsKey] ?? "4");
            var copiedRows = await DataCopyHelper.CopyData(sourceConn, destConn, maxCopyThreads);
            Console.WriteLine($"Copied {copiedRows} Total Rows");

            int mismatches = await DataValidationHelper.ValidateRowCounts(sourceConn, destConn);
            Console.WriteLine(mismatches == 0
                ? "Validation passed: all table row counts match."
                : $"Validation complete: {mismatches} table(s) with mismatched row counts.");
        }

        Console.WriteLine("Done");
        Console.ReadLine();
    }
}
