using Cake.Core.Diagnostics;
using Cake.Core.IO;
using Cake.Core;
using Cake.Frosting;
using System.Diagnostics;
using Cake.Common.IO;

namespace LogsCollector
{
    public sealed class LogContext(ICakeContext context) : FrostingContext(context)
    {
        private static DirectoryPath __root = new DirectoryPath(System.IO.Directory.GetCurrentDirectory())
            .GetParent()
            .GetParent()
            .GetParent();
        private static readonly FilePath __outputFile = __root.Combine("output").CombineWithFilePath($"output_{DateTime.UtcNow:MMddyyyyHHmmss}");

        private class LogsToFileAggregator : ICakeLog
        {
            private readonly ICakeLog _baseLog;
            public LogsToFileAggregator(ICakeLog log)
            {
                _baseLog = log;
                try
                {
                    // TODO: think about switching file name each hour instead of each run?
                    Trace.AutoFlush = true;
                    _ = Directory.CreateDirectory(__outputFile.GetDirectory().FullPath);
                    Trace.Listeners.Add(
                        new TextWriterTraceListener(
                            File.CreateText(__outputFile.FullPath)));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n\n\n==========================> The trace listener creating has been faield: {ex}\n\n\n");

                    /* ignore for now */
                }
            }
            public Verbosity Verbosity { get => _baseLog.Verbosity; set => _baseLog.Verbosity = value; }

            public void Write(Verbosity verbosity, LogLevel level, string format, params object[] args)
            {
                try
                {
                    if (verbosity <= Verbosity)
                    {
                        if (level > LogLevel.Error)
                        {
                            Trace.WriteLine(message: string.Format(format, args));
                        }
                        else
                        {
                            Trace.TraceError(message: string.Format(format, args));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n\n\n==========================> Trace write to file has been failed: {ex}\n\n\n");

                    /* Ignore for now */
                }

                _baseLog.Write(verbosity, level, format, args);
            }
        }

        public override ICakeLog Log => new LogsToFileAggregator(base.Log);
        public DirectoryPath Root => __root;
    }
}
