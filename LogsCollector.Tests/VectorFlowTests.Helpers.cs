using GraphQL;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.SystemTextJson;
using Microsoft.Azure.Cosmos;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Xunit.Abstractions;
using static LogsTransmitterFunction.LogsTransmitterFunction;

namespace LogsCollector.Tests
{
    public partial class VectorFlowTests
    {
        public static class Cosmos
        {
            public const string DbName = "LogDb";
            public const string ContainerName = "CollectedLogs";
            public const string SourceContainerName = "SourceLogs";
            private static readonly string __connectionString;

            static Cosmos()
            {
                __connectionString = Environment.GetEnvironmentVariable("COSMOSDB_CONNECTION_STRING") ?? throw new InvalidOperationException("COSMOSDB_CONNECTION_STRING");
            }

            public static async Task PrepareCosmos(string containerName = ContainerName)
            {
                using var client = new CosmosClient(connectionString: __connectionString);
                var container = client.GetContainer(DbName, containerName);
                try
                {
                    await container.DeleteContainerAsync();
                }
                catch (Microsoft.Azure.Cosmos.CosmosException)
                {
                    // ignore
                }
                catch (System.Net.Http.HttpRequestException)
                {
                    // cosmosdb is not accessible
                    throw;
                }
            }

            public static async Task InsertRecords(IEnumerable<string> messages, string containerName = ContainerName)
            {
                using var client = new CosmosClient(connectionString: __connectionString);
                await client.CreateDatabaseIfNotExistsAsync(DbName);
                var database = client.GetDatabase(DbName);
                await database.CreateContainerIfNotExistsAsync(containerName, "/id");
                var container = client.GetContainer(DbName, containerName);

                foreach (var message in messages)
                {
                    await container.CreateItemAsync(new { id = Guid.NewGuid(), message = message });
                }
            }

            private static void ValidateRecods(Action<IEnumerable<string>> validateMessagesFunc)
            {
                using var client = new CosmosClient(connectionString: __connectionString);
                var container = client.GetContainer(DbName, ContainerName);
                var querable = container.GetItemLinqQueryable<LogBatch>(allowSynchronousQueryExecution: true).ToList();
                var messages = (querable.SelectMany(i => i.Logs)!.Cast<Newtonsoft.Json.Linq.JObject>().ToList()).Select(i => i.GetValue("full_message")!.ToString());
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

            public static void RenameFile(string originalFullPath, string newFullPath) => File.Copy(originalFullPath, newFullPath, overwrite: true);

            public static void UpdateConfig(string path, string filter, params string[] newValues)
            {
                ArgumentNullException.ThrowIfNull(newValues, paramName: nameof(newValues));
                var configFileLines = new LinkedList<string>(File.ReadAllLines(path));
                var node = FindLinkedListNode(configFileLines, (str) => str.Contains(filter))
                    ?? throw new InvalidOperationException($"The '{filter}:' node has not been found in config file."); // the source name
                foreach (var newValue in newValues)
                {
                    node = configFileLines.AddAfter(node, newValue);
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

                var json = JsonSerializer.Deserialize<LogLine>(line)!;
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
                string? waitUntil,
                ConcurrentQueue<string> errorStdOutput,
                TimeSpan? timeout = null)
            {
                var process = RunPs1Script(arguments, workingDirectory, errorStdOutput, out var output, out _);
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
                bool inWindow = false /* true is for debug purpose */)
            {
                output = null;
                error = null;
                var processStartInfo = new ProcessStartInfo
                {
                    FileName = "powershell.exe",
                    Arguments = arguments
                };
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

                var process = new Process
                {
                    StartInfo = processStartInfo
                };
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

            public static void KillTool(ConcurrentQueue<string> errorStdOutput, string name)
            {
                using (var killProcess = RunPs1Script(
                    arguments: @$"-Command ""Get-Process -Name {name} | Stop-Process""",
                    errorStdOutput: errorStdOutput,
                    workingDirectory: __originalAzureFunctionProjectDirectory,
                    waitUntil: null)) { /* kill process that listens port 8686 */ }
                Thread.Sleep(TimeSpan.FromSeconds(5)); // sanitize sleep
            }

            public static void EnsureVectorIsAvailable(ConcurrentQueue<string> errorStdOutput)
            {
                using (var killProcess = RunPs1Script(
                    arguments: @$"-Command ""vector --version""",
                    workingDirectory: __originalAzureFunctionProjectDirectory,
                    errorStdOutput: errorStdOutput,
                    waitUntil: "vector 0.")) { /* ensure vector is available on the machine */ }
            }

            public static Process SpawnFunction(string source, ConcurrentQueue<string> errorStdOutput, int port, string waitUntil) =>
                RunPs1Script(
                    arguments: @$"-Command ""func host start --port {port} --debug""",
                    workingDirectory: source,
                    errorStdOutput: errorStdOutput,
                    waitUntil: waitUntil);
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
                            ).Length > 0,
                        timeout))
                    {
                        if (!throwIfFound)
                        {
                            Assert.Fail($"The output must match func ({logTag}), but was not during {timeout}\n({string.Join("\n\t\t\t", output)})");
                        }
                    }

                    // TODO: merge these 2 Assert.Fails
                    if (throwIfFound && foundMessages != null && foundMessages.Length > 0)
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
                AssertOutput(output, timeout, logTag: expectedValue, expectedCount, throwIfFound, testOutputHelper, (str) => str.Contains(expectedValue));

            public static void AssertOutput(
                ConcurrentQueue<string> output,
                TimeSpan timeout,
                ITestOutputHelper? testOutputHelper = null,
                params string[] notExpectedValues)
            {
                ArgumentNullException.ThrowIfNull(output, nameof(output));
                ArgumentNullException.ThrowIfNull(notExpectedValues, nameof(notExpectedValues));
                ArgumentOutOfRangeException.ThrowIfZero(notExpectedValues.Length, nameof(notExpectedValues.Length));

                AssertOutput(
                    output,
                    timeout,
                    logTag: $"[{string.Join(",", notExpectedValues)}]",
                    expectedCount: null,
                    throwIfFound: true,
                    testOutputHelper, (str) => notExpectedValues.Any(i =>
                        str.Contains(i) &&
                        // TODO: should gone after reconfiguration tls
                        !str.Contains("verify_certificate")));
            }
        }

        public static class GraphQLHelper
        {
            private const string GraphQLUri = "http://127.0.0.1:8686/graphql";

            public static async Task ValidateThroughApi(Action<Root> action)
            {
                ArgumentNullException.ThrowIfNull(action, nameof(action));

                var options = new GraphQLHttpClientOptions
                {
                    EndPoint = new Uri(GraphQLUri)
                };

                using var client = new GraphQLHttpClient(options, new SystemTextJsonSerializer());

                var request = new GraphQLRequest
                {
                    Query = @"query {
  sources {
    edges {
      node {
        componentId
        metrics {
          sentEventsTotal {
            sentEventsTotal
          }
          # Total events that the source has received.
          receivedEventsTotal {
            receivedEventsTotal
          }
        }
      }
    }
  }

  sinks {
    edges {
      node {
        componentId
        metrics {
          sentEventsTotal {
            sentEventsTotal
          }
          receivedEventsTotal {
            receivedEventsTotal
          }
        }
      }
    }
  }
}"
                };

                var response = await client.SendQueryAsync<Root>(request);
                action(response.Data);
            }

            public class Root
            {
                public SourcesResponse? Sources { get; set; }
                public SinksResponse? Sinks { get; set; }
            }

            public class SourcesResponse
            {
                public List<SourceEdge>? Edges { get; set; }
            }

            public class SourceEdge
            {
                public SourceNode? Node { get; set; }
            }

            public class SourceNode
            {
                public string? ComponentId { get; set; }
                public SourceMetrics? Metrics { get; set; }
            }

            public class SourceMetrics
            {
                public SentEventsTotalSummary? SentEventsTotal { get; set; }
                public ReceivedEventsTotalSummary? ReceivedEventsTotal { get; set; }
            }

            public class SentEventsTotalSummary
            {
                public decimal? SentEventsTotal { get; set; }
            }

            public class ReceivedEventsTotalSummary
            {
                public decimal? ReceivedEventsTotal { get; set; }
            }

            public class SinksResponse
            {
                public List<SinkEdge>? Edges { get; set; }
            }

            public class SinkEdge
            {
                public SinkNode? Node { get; set; }
            }

            public class SinkNode
            {
                public string? ComponentId { get; set; }
                public SinkMetrics? Metrics { get; set; }
            }

            public class SinkMetrics
            {
                public SentEventsTotalSummary? SentEventsTotal { get; set; }
                public ReceivedEventsTotalSummary? ReceivedEventsTotal { get; set; }
            }
        }
    }
}
