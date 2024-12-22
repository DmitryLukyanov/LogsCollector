using Cake.Frosting;
using Cake.Powershell;
using System.Diagnostics;

namespace LogsCollector
{
    internal class Program
    {
        static int Main(string[] args)
        {
            // The function emulator requires some time to be launched
            // TODO: find a better way
            Thread.Sleep(TimeSpan.FromSeconds(10));

            return new CakeHost()
                .UseContext<LogContext>()
                .Run(args);
        }
    }

    [TaskName("Default")]
    public class DefaultTask : FrostingTask<LogContext>
    {
        public override void Run(LogContext context)
        {
            var workingDirectory = context.Root;
            var script = workingDirectory.CombineWithFilePath("Launch.ps1");

            try
            {
                // TODO: check on timeout if long run?
                var result = context.StartPowershellFile(
                    path: script,
                    settings: new PowershellSettings
                    {
                        WorkingDirectory = workingDirectory,
                        BypassExecutionPolicy = true,
                        LogOutput = true,
                        // Timeout = (int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                        ExceptionOnScriptError = true
                    })
                    .ToList();

                if (result.Any(i => i.ToString().Contains("Failed", StringComparison.OrdinalIgnoreCase)))
                {
                    // TODO: find a better way
                    throw new InvalidOperationException(string.Concat(result));
                }
            }
            catch (Exception ex)
            {
                Debugger.Launch();
                Console.WriteLine(ex.ToString());
                throw;
            }

            base.Run(context);
        }
    }
}
