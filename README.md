# copygpdatabase

`copygpdatabase` is a .NET 8 command-line tool that clones an existing Microsoft Dynamics GP SQL database into a new database, then copies all data from the source to the destination.

The primary use case is GP to Business Central (BC) migrations where you want to retain your full GP data history in a separate database for reporting, audit, or archive purposes.

While the initial reason for this was a way to get GP data into Azure SQL the code is not specific to GP. This could be used for almost any database.

## Why Create This
The original idea for this came from a conversation about what to do with data in GP that is needed for long term retention policies or reporting. Obviously there are solutions to get some data into BC but there is a LOT of other data in GP. The problem is that GP has encrypted views and stored procedures and Azure SQL doesn't allow them, so a GP database can't be restored to Azure SQL. One can use a VM with SQL installed but the cost of a VM is a lot more than the cost of a low end Azure SQL instance.

So, when I was out on leave for major surgery in the summer of 2025 I started working on this utility. I only recently got around to finishing it. I hope some people find it useful.

Some ideas I have on what could be done:
- Create an MCP Server to plug into BC Copilot grounded on this data.
- Combine this data with BC data in Power BI.
- Create a custom connector in the Power Platform to allow querying this data.

I have tested it with local instances of SQL databases, I tested the creation code going to Azure SQL.

## What It Does

- Connects to an existing destination SQL database.
- Recreates the structure of the source GP database (tables and basic schema) in the destination database.
   - Primary Keys are maintained for each table, if there is one.
- Copies all tables and data from the source database into the destination database.
- Checks record counts and reports any tables that don't match.
- Leaves the destination database in a state that is easy to query and archive.

### What about updates?
The utility does use a SQL Merge statement. So, if you run it multiple times data changes will migrate over. ***BUT*** Structure changes are not migrated. If a table structure changes you will need to drop the destination table manually.

## What It Does **Not** Do (Important Limitations)

The destination database is intentionally created without some schema elements so that the copy is fast, simple, and safe to use as an archive:

- No indexes are created (clustered or nonclustered).
- No foreign key constraints are created.
- No unique constraints are created.
- Identity settings may not be preserved exactly as on the source (for example, identity seeds and increments).

This means the destination database is **not** suitable as a drop-in replacement for your production GP database or for write-heavy transactional use. It is intended primarily for read-only access (reporting, BI, ad-hoc queries, long-term storage).

## Typical GP → BC Migration Scenario

This tool is typically used as part of a GP to Business Central migration plan:

- Keep GP data in its entirety in a standalone SQL database.
- Allow users, auditors, or reporting tools to query the legacy data after cutover to BC.
- Avoid carrying over all historical data into BC while still preserving it for reference.

You can also use the tool outside of GP → BC migrations any time you need a lightweight full-data copy of a GP database for analysis or archiving. The data copy does a merge so any updates that occur between run will be migrated over.

## Requirements

- .NET 8 SDK (for building from source).
- Access to a SQL Server or Azure SQL instance that hosts the GP source database.
- Access to a SQL Server or Azure SQL instance that hosts the destination database.
- Permissions to create and modify tables and data in the destination database.

## Configuration

The tool is configured via an `appsettings.json` file that is copied to the build output.

Configuration Settings in [appsettings.json](/src/CloneDatabase/CloneGPDatabase/appsettings.json)

- ***MaxCopyThreads*** - Default = 4 The number of threads to use on the copy operation. More is faster but consumes more processor.
- ***LogFilePath*** - Default = "clone_log.txt". The path to the log file
- ***SOURCE_SQL*** - Connection string to the source SQL Database
- ***DESTINATION_SQL*** - Connection string to the destination SQL Database

Refer to the comments or examples in `appsettings.json` in this repository for the exact shape of the configuration.

## Usage

Once configured, you can compile and run the command-line tool from PowerShell or a terminal:

1. Navigate to the solution folder and build it:
   - `cd src/CloneDatabase`
   - `dotnet build CloneDatabase.sln -c Release`

2. Run the command-line project:
   - `cd CloneGPDatabase`
   - `dotnet run --project CloneGPDatabase.csproj`

The tool will connect to the configured SQL instances, replicate the schema in the destination database (without indexes/constraints), and copy all table data from source to destination.

Exact options and behavior may vary by version; review the source code and configuration to understand the workflow for your environment.

## Safety and Best Practices

- Always test against non-production databases first.
- Ensure you have valid backups of your source and destination SQL instances before running the tool.
- Consider placing the destination database in a read-only state once the copy is complete if it is only used for archive/reporting.

## Contributing

Contributions are welcome. If you find a bug or want to extend the tool (for example, adding optional index/constraint creation or progress reporting), feel free to fork the repo. If you find a bug and fix it feel free to submit a pull request.

`copygpdatabase` and it's code and anything related to it are provided as-is with no implied warranty or support. If you don't understand C# code or compiling a console application use with caution.
