using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit.Abstractions;
using static LogsTransmitterFunction.LogsTransmitterFunction;

namespace LogsCollector.Tests
{
    public partial class VectorFlowTests
    {
        public static class Cosmos
        {
            public static void DropCollection()
            {
                const string cosmosDbConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
                const string dbName = "LogDb";
                const string containerName = "CollectedLogs";
                using var client = new CosmosClient(connectionString: cosmosDbConnectionString);
                var container = client.GetContainer(dbName, containerName);
                try
                {
                    container.DeleteContainerAsync().GetAwaiter().GetResult();
                }
                catch (Microsoft.Azure.Cosmos.CosmosException)
                {
                    // ignore
                }
            }

            private static void ValidateRecods(Action<IEnumerable<string>> validateMessagesFunc)
            {
                const string cosmosDbConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

                const string dbName = "LogDb";
                const string containerName = "CollectedLogs";
                using var client = new CosmosClient(connectionString: cosmosDbConnectionString);
                var container = client.GetContainer(dbName, containerName);
                var querable = container.GetItemLinqQueryable<LogBatch>(allowSynchronousQueryExecution: true).ToList();
                var messages = (querable.SelectMany(i => i.Logs)!.Cast<Newtonsoft.Json.Linq.JObject>().ToList()).Select(i => i.GetValue("message")!.ToString());
                validateMessagesFunc(messages);
            }

            public static void ValidateRecods(IEnumerable<string> values)
            {
                ValidateRecods((messages) =>
                {
                    foreach (var value in values)
                    {
                        Assert.Contains(
                            value,
                            messages,
                            EqualityComparer<string>.Create(
                                (a, b) =>
                                {
                                    if (a == b && a == null) return true;
                                    if (a == null || b == null) return false;

                                    return a.Contains(b);
                                }));
                    }
                });
            }
        }

        public static class Markup
        {
            const string StringMarker = "____";
            public static string CreateMarkeredLine<T>(T identificator, bool withMarker = true, int? recordSize = null) => $"{(withMarker ? StringMarker : string.Empty)}{identificator}_{Guid.NewGuid()}{(recordSize.HasValue ? new string('a', recordSize.Value) : string.Empty)}";
        }

        public static class FileSystem
        {
            public static string CreateDirectory(params string[] segments)
            {
                ArgumentNullException.ThrowIfNull(segments, paramName: nameof(segments));
                var path = Path.Combine(segments);
                _ = Directory.CreateDirectory(path);
                return path;
            }

            public static string CopyDirectory(string original, params string[] segments)
            {
                ArgumentNullException.ThrowIfNull(segments, paramName: nameof(segments));
                var newDirectory = Path.Combine(segments);
                FileSystem.CopyDirectory(original, newDirectory);
                return newDirectory;
            }

            public static string CopyFile(string original, params string[] segments)
            {
                ArgumentNullException.ThrowIfNull(segments, paramName: nameof(segments));
                var newFile = Path.Combine(segments);
                File.Copy(original, newFile);
                return newFile;
            }

            public static void UpdateConfig(string path, string filter, params string[] newValues)
            {
                ArgumentNullException.ThrowIfNull(newValues, paramName: nameof(newValues));
                var configFileLines = new LinkedList<string>(File.ReadAllLines(path));
                var sourcesNode = FindLinkedListNode(configFileLines, (str) => str.Contains(filter)); // the source name
                if (sourcesNode == null)
                {
                    throw new InvalidOperationException("The 'sources:' node has not been found in config file.");
                }
                foreach (var newValue in newValues)
                {
                    sourcesNode = configFileLines.AddAfter(sourcesNode, newValue);
                }

                File.WriteAllLines(path, configFileLines);
            }

            private static LinkedListNode<string>? FindLinkedListNode(LinkedList<string> list, Func<string, bool> func)
            {
                var current = list.First;
                while (current != null && current.Value != null && !func(current.Value))
                {
                    current = current.Next;
                }
                return current;
            }

            private static void CopyDirectory(string source, string destination)
            {
                _ = Directory.CreateDirectory(destination);
                foreach (string dirPath in Directory.GetDirectories(
                    source,
                    "*",
                    SearchOption.AllDirectories))
                    Directory.CreateDirectory(dirPath.Replace(source, destination));

                foreach (string newPath in Directory.GetFiles(
                    source,
                    "*.*",
                    SearchOption.AllDirectories))
                    File.Copy(newPath, newPath.Replace(source, destination));
            }
        }

        public static class Parser
        {
            public static bool IsLineValid(string line, Func<string> expectedMessageFunc, bool? skipCondition = null)
            {
                if (skipCondition.HasValue && skipCondition.Value)
                {
                    return true;
                }

                var json = JsonConvert.DeserializeObject<LogLine>(line)!;
                return
                    json.File == "logs\\raw.log" &&
                    json.Host != null &&
                    json.Message.Contains(expectedMessageFunc()) &&
                    json.SourceType == "file" &&
                    json.Timestamp != default;
            }
        }

        public static class Powershell
        {
            public static Process RunPs1Script(
                string arguments,
                string workingDirectory,
                string waitUntil,
                ConcurrentQueue<string> errorStdOutput,
                TimeSpan? timeout = null)
            {
                var process = RunPs1Script(arguments, workingDirectory, errorStdOutput, out var output, out var error, timeout: timeout);
                if (waitUntil != null)
                {
                    AssertStream.AssertOutput(output!, waitUntil, timeout: timeout ?? TimeSpan.FromSeconds(30));
                }
                return process;
            }

            public static Process RunPs1Script(
                string arguments,
                string workingDirectory,
                ConcurrentQueue<string> errorStdOutput,
                out ConcurrentQueue<string>? output,
                out ConcurrentQueue<string>? error,
                bool inWindow = false,
                TimeSpan? timeout = null)
            {
                output = null;
                error = null;
                var processStartInfo = new ProcessStartInfo();
                processStartInfo.FileName = "powershell.exe";
                processStartInfo.Arguments = arguments;
                if (inWindow)
                {
                    processStartInfo.UseShellExecute = true;
                    processStartInfo.RedirectStandardOutput = false;
                    processStartInfo.RedirectStandardError = false;
                }
                else
                {
                    processStartInfo.UseShellExecute = false;
                    processStartInfo.RedirectStandardOutput = true;
                    processStartInfo.RedirectStandardError = true;
                }
                processStartInfo.WorkingDirectory = workingDirectory;

                var process = new Process();
                process.StartInfo = processStartInfo;
                ConcurrentQueue<string> innerOutput = new();
                ConcurrentQueue<string> innerError = new();
                if (!inWindow)
                {
                    process.OutputDataReceived += (sender, args) => AppendIfNotEmpty(innerOutput, args.Data, isErrorStd: false);
                    process.ErrorDataReceived += (sender, args) => AppendIfNotEmpty(innerError, args.Data, isErrorStd: true);
                }
                process.Start();
                if (!inWindow)
                {
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();
                }

                output = innerOutput;
                error = innerError;

                return process;

                void AppendIfNotEmpty(ConcurrentQueue<string> queue, string? value, bool isErrorStd = false)
                {
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        queue.Enqueue(value);
                        if (isErrorStd)
                        {
                            // validate warnings
                            errorStdOutput.Enqueue(value);
                        }
                    }
                }
            }

            public static void KillVector(ConcurrentQueue<string> errorStdOutput)
            {
                using (var killProcess = RunPs1Script(
                    arguments: @$"-Command ""Get-Process -Name vector | Stop-Process""",
                    errorStdOutput: errorStdOutput,
                    workingDirectory: __originalAzureFunctionProjectDirectory,
                    waitUntil: null!)) { /* kill process that listens port 8686 */ }
                Thread.Sleep(TimeSpan.FromSeconds(5)); // sanitize sleep
            }

            public static void EnsureVectorIsAvailable(ConcurrentQueue<string> errorStdOutput)
            {
                using (var killProcess = RunPs1Script(
                    arguments: @$"-Command ""vector --version""",
                    workingDirectory: __originalAzureFunctionProjectDirectory,
                    errorStdOutput: errorStdOutput,
                    waitUntil: "vector 0."!)) { /* ensure vector is available on the machine */ }
            }

            public static Process SpawnFunction(ConcurrentQueue<string> errorStdOutput, int port) =>
                RunPs1Script(
                    arguments: @$"-Command ""func host start --port {port} --debug""",
                    workingDirectory: __originalAzureFunctionProjectDirectory,
                    errorStdOutput: errorStdOutput,
                    waitUntil: "LogsTransmitter: [POST]");
        }

        public static class AssertStream
        {
            public static void AssertOutput(
                ConcurrentQueue<string> output,
                TimeSpan timeout,
                string logTag,
                int? expectedCount = null,
                bool throwIfFound = false,
                ITestOutputHelper? testOutputHelper = null,
                params Func<string, bool>[] funcs)
            {
                ArgumentNullException.ThrowIfNull(funcs);

                testOutputHelper?.WriteLine($"\n--> AssertOutput has been started ({logTag}), Timeout: {timeout}. Expected count: {expectedCount?.ToString() ?? "<empty>"}");

                var duration = Stopwatch.StartNew();

                foreach (var func in funcs)
                {
                    string[]? foundMessages = null;
                    if (!SpinWait.SpinUntil(
                        () =>
                            (foundMessages = output
                                .Where(i =>
                                {
                                    try
                                    {
                                        return func(i);
                                    }
                                    catch
                                    {
                                        return false;
                                    }
                                })
                                .ToArray()
                            ).Any(),
                        timeout))
                    {
                        if (!throwIfFound)
                        {
                            Assert.Fail($"The output must match func ({logTag}), but was not during {timeout} ({string.Join(",", output)})");
                        }
                    }

                    // TODO: merge these 2 Assert.Fails
                    if (throwIfFound && foundMessages != null && foundMessages.Any())
                    {
                        Assert.Fail($"WARNING: The warning output message ({string.Join(",", foundMessages)}) has been detected.");
                    }

                    if (expectedCount != null)
                    {
                        if (!SpinWait.SpinUntil(
                            () => object.Equals(expectedCount, output.Count(i => func(i))),
                            timeout))
                        {
                            Assert.Fail($"Expected: {expectedCount}, but actual: {output.Count(i => func(i))}");
                        }
                    }
                }
                duration.Stop();
                testOutputHelper?.WriteLine($"==> AssertOutput has been completed ({logTag}). Duration: {duration}\n");
            }

            public static void AssertOutput(ConcurrentQueue<string> output, string expectedValue, TimeSpan timeout, int? expectedCount = null, bool throwIfFound = false, ITestOutputHelper? testOutputHelper = null) =>
                AssertOutput(output, timeout, logTag: expectedValue, expectedCount, throwIfFound, testOutputHelper , (str) => str.Contains(expectedValue));
        }
    }
}
