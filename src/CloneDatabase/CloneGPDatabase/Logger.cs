using System;
using System.IO;

namespace CloneGPDatabase
{
    internal static class Logger
    {
        private static string _logFilePath;
        private static readonly object _lock = new object();

        public static void Initialize(string logFilePath)
        {
            _logFilePath = logFilePath;
            // Create or overwrite the log file with a start header.
            File.WriteAllText(_logFilePath, $"Log started at {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}");
        }

        public static void Log(string message)
        {
            string entry = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.WriteLine(entry);
            if (_logFilePath == null) return;
            // Lock ensures concurrent threads don't interleave writes to the file.
            lock (_lock)
            {
                File.AppendAllText(_logFilePath, entry + Environment.NewLine);
            }
        }
    }
}
