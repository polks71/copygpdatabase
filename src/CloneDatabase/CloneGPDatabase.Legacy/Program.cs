using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloneGPDatabase.Legacy
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var sourceServer = ConfigurationManager.AppSettings["SourceServer"];
        }
    }
}
