using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloneGPDatabase
{
    internal static class CloneConstants
    {
        public const string DestinationSqlConnectionStringKey = "DESTINATION_SQL";
        public const string SourceSqlConnectionStringKey = "SOURCE_SQL";
        public const string SkipIfExistsKey = "SkipIfExists";
    }
}
