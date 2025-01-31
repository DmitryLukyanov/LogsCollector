using DotNetEnv;
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit.Abstractions;

namespace LogsCollector.Tests
{
    // TODO: clarify timeout values
    public partial class VectorFlowTests : IDisposable
    {
        #region static
        private static readonly string __testDirectory = Path.GetFullPath(
            Path.Combine(
                Directory.GetCurrentDirectory(),
                "..",
                "..",
                ".."));
        private static readonly string __originalAzureFunctionProjectDirectory = Path.GetFullPath(
            Path.Combine(
                __testDirectory,
                "..",
                "LogsTransmitterFunction"));
        private static readonly string __originalAzureFunctionHttpSourceProjectDirectory = Path.GetFullPath(
            Path.Combine(
                __testDirectory,
                "..",
                "LogsSourceFunction"));
        private static readonly string __originalProjectDirectory = Path.GetFullPath(
            Path.Combine(
                __testDirectory,
                "..",
                "LogsCollector"));
        private static readonly string __originalConfigDirectory = Path.GetFullPath(
            Path.Combine(
                __originalProjectDirectory,
                "config"));
        private static readonly string __originalConfigWithHttpSource = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_http_source.yaml"));
        private static readonly string __originalConfig = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector.yaml"));
        private static readonly string __originalConfigWithDataDogSink = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_datadog_sink.yaml"));
        private static readonly string __originalConfigWithHttpSourceAndDataDogSink = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_http_source_to_datadog.yaml"));

        private static readonly string __originalInternalLogsConfig = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_internal_logs.yaml"));

        private static readonly string __originalLogsConfigWithDedupeTransform = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_dedupe_transform.yaml"));

        private static readonly string __originalLogsConfigWithHttpSourceAndUnwrapArrayOfMessages = Path.GetFullPath(
            Path.Combine(
                __originalConfigDirectory,
                "vector_with_http_source_and_unwrap_transform.yaml"));


        private static readonly string __originalLaunchPs1Path = Path.Combine(__originalProjectDirectory, "Launch.ps1");
        #endregion

        private const int SinkHttpFunctionPort = 7245;
        private readonly ConcurrentQueue<string> _stdError;
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly string _testRunConfig;
        private readonly string _testRunDirectory;
        private readonly string _testRunLaunchPs1;
        private readonly string _testRunLogDirectory;
        private readonly string _testRunRawLog;
        private readonly string _testRunConfigDirectory;

        public VectorFlowTests(ITestOutputHelper testOutputHelper)
        {
            Assert.True(Env.Load(path: Path.Combine(__testDirectory, ".env")).Any(), "The environment variables are not configured");

            _testOutputHelper = testOutputHelper;
            _stdError = new();
            Powershell.EnsureVectorIsAvailable(_stdError);

            /* 
             * sometimes this process is not killed since previous run and this leads to this error:
             * - Only one usage of each socket address 
             */
            Powershell.KillTool(_stdError, "vector");
            /*
             * isolated function process is not killed with powershell shell for some reason, so do it manually
             */
            Powershell.KillTool(_stdError, "func");

            // prepare test
            _testRunDirectory = FileSystem.CreateDirectory(__testDirectory, $"run_{DateTime.UtcNow:MMddyyyyHHmmss}");
            _testRunLogDirectory = FileSystem.CreateDirectory(_testRunDirectory, "logs");
            _testRunRawLog = Path.Combine(_testRunLogDirectory, "raw.log");
            _testRunLaunchPs1 = FileSystem.CopyFile(__originalLaunchPs1Path, _testRunDirectory, Path.GetFileName(__originalLaunchPs1Path));

            _testRunConfigDirectory = Directory.CreateDirectory(Path.Combine(_testRunDirectory, "config")).FullName;
            _testRunConfig = FileSystem.CopyFile(
                original: __originalConfig,
                _testRunConfigDirectory, "vector.yaml");
        }

        public void Dispose()
        {
            if (_stdError != null && !_stdError.IsEmpty)
            {
                AssertStream.AssertOutput(
                    _stdError!,
                    TimeSpan.FromSeconds(10),
                    testOutputHelper: _testOutputHelper,
                    // specific check
                    "Only one usage of each socket address",
                    // just check on unhandled ERROR
                    " ERROR vector",
                    // warning mainly related to unsafe configuration
                    " WARN");
            }
        }

        [SkippableTheory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        public async Task Ensure_config_is_valid(int testCase)
        {
            var config = testCase switch
            {
                1 => null,
                2 => __originalConfigWithHttpSource,
                3 => Environment.GetEnvironmentVariable("DATADOG_API_KEY") == null
                    ? throw new Xunit.SkipException("DATADOG_API_KEY must be specified")
                    : __originalConfigWithHttpSourceAndDataDogSink,
                4 => Environment.GetEnvironmentVariable("DATADOG_API_KEY") == null
                    ? throw new Xunit.SkipException("DATADOG_API_KEY must be specified")
                    : __originalConfigWithDataDogSink,
                5 => __originalInternalLogsConfig,
                6 => __originalLogsConfigWithDedupeTransform,
                7 => __originalLogsConfigWithHttpSourceAndUnwrapArrayOfMessages,
                _ => throw new NotImplementedException()
            };

            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            if (config != null)
            {
                FileSystem.RenameFile(config, _testRunConfig);
            }

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out _);

            AssertStream.AssertOutput(output!, "Validated", timeout: TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task Ensure_logs_are_read_from_file_created_beforehand()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            var values = new[]
            {
                "1_" + Guid.NewGuid(),
                "2_" + Guid.NewGuid(),
                "3_" + Guid.NewGuid(),
                "4_" + Guid.NewGuid(),
                "5_" + Guid.NewGuid()
            };
            File.WriteAllText(_testRunRawLog, $@"{values[0]}
{values[1]}
{values[2]}
{values[3]}
{values[4]}
");

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[1]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[2]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[3]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[4]}\r"));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: 1);
            Cosmos.ValidateRecods(values);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 5M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 5M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_logs_are_read_when_batch_size_is_less_than_number_of_lines_in_the_file()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            FileSystem.UpdateConfig(
                path: _testRunConfig,
                filter: "http:",
                $@"    batch:",
                $@"      max_events: 10");

            var values = Enumerable.Range(1, 51).Select(i => Markup.CreateMarkeredLine(i)).ToArray();
            File.WriteAllText(_testRunRawLog, string.Join("\r\n", values.Append(string.Empty)));

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                testOutputHelper: _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}\r"), // first element
                str => Parser.IsLineValid(str, () => $"{values[values.Length / 2]}\r"), // something in the middle
                str => Parser.IsLineValid(str, () => $"{values[^1]}\r")); // last element

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: 6); // 6 different http calls
            Cosmos.ValidateRecods(values);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 51M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 51M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Theory]
        [InlineData("\\n", "\n")]
        [InlineData("\\r\\n", "\r\n")]
        public async Task Ensure_logs_are_read_when_custom_delimiter_is_specified(string delimiter, string effectiveDelimiter)
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            FileSystem.UpdateConfig(path: _testRunConfig, filter: "fileIn:", $@"    line_delimiter: ""{delimiter}""");

            int[] rawValues = [0, 1, 2];
            var values = rawValues.Select(i => Markup.CreateMarkeredLine(i)).ToArray();

            File.WriteAllText(_testRunRawLog, string.Concat(string.Join(effectiveDelimiter, values), effectiveDelimiter));

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                testOutputHelper: _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}"),
                str => Parser.IsLineValid(str, () => $"{values[1]}"),
                str => Parser.IsLineValid(str, () => $"{values[2]}"));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: 1);
            Cosmos.ValidateRecods(values);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 3M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 3M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_logs_are_read_when_multiline_logs_is_specified()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            FileSystem.UpdateConfig(
                path: _testRunConfig,
                filter: "fileIn:",
                $@"    multiline:",
                $@"      start_pattern: ""^[0-9]_""",
                $@"      condition_pattern: ""^part of\\s+[0-9].*""",
                $@"      mode: ""continue_through""",
                $@"      timeout_ms: 5000");

            string[] rawValues = [
                "0", // record 1
                "1", "part of 1.1", "part of 1.2", // record 2
                "2", "part of 2.1", "part of 2.2", "part of 2.3" // record 3
            ];
            var values = rawValues.Select(i => Markup.CreateMarkeredLine(i, withMarker: false)).ToArray();
            string[] expectedValues =
                [
                    values[0],
                    string.Join("\r\n", values[1], values[2], values[3]),
                    string.Join("\r\n", values[4], values[5], values[6], values[7])
                ];

            File.WriteAllLines(_testRunRawLog, values);

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                testOutputHelper: _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{expectedValues[0]}"),
                str => Parser.IsLineValid(str, () => $"{expectedValues[1]}"),
                str => Parser.IsLineValid(str, () => $"{expectedValues[2]}"));

            AssertStream.AssertOutput(
                error!,
                "200 OK",
                TimeSpan.FromSeconds(10),
                expectedCount: 2); // the first batch will consist of items [0, 1], the second => [2]
            Cosmos.ValidateRecods(values);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 3M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 3M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        /*
         * WARNING:
         * Apperently such way will emit 14 events (instead 12) that is more than actually required, 
         * but good part that this is a synthetic case that is not supposed to happen.
         * The result is: 1, 2, 3, 3 (not expected), 0, 1, 2, 3, 4(not expected), 0, 1, 2, 3, 4
         */
        [Fact]
        public async Task Ensure_logs_are_read_when_updating_first_line_read_all_lines_again()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            using var cancellationTokenSource = new CancellationTokenSource();
            using (var stream = File.Create(_testRunRawLog)) { /* close handle */ }

            var values = new int[][] { [1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3, 4] }
                .Select(i => i.Select(ii => Markup.CreateMarkeredLine(ii)).ToArray())
                .ToArray(); /* also represents expected output */
            var reset = new AutoResetEvent(false);
            var confirmation = new AutoResetEvent(false);
            var task = Task.Factory.StartNew(() =>
            {
                foreach (var attempt in values)
                {
                    var duration = Stopwatch.StartNew();
                    var attemptTag = string.Join(",", attempt);

                    File.WriteAllLines(_testRunRawLog, attempt); // fully replace the file
                    confirmation.Set();
                    reset.WaitOne();
                }
                confirmation.Set(); // release waiting in last iteration
            },
            cancellationTokenSource.Token);
            confirmation.WaitOne(); // first iteration

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            int expectedHttpCalls = 1;
            foreach (var value in values)
            {
                AssertStream.AssertOutput(
                    output!,
                    timeout: TimeSpan.FromSeconds(30),
                    logTag: $"Iteration_{expectedHttpCalls}", // expectedHttpCalls represents iteration too
                    expectedCount: null,
                    throwIfFound: false,
                    testOutputHelper: _testOutputHelper,
                    str => Parser.IsLineValid(str, () => $"{value[0]}\r"),
                    str => Parser.IsLineValid(str, () => $"{value[1]}\r", skipCondition: value.Length < 2),
                    str => Parser.IsLineValid(str, () => $"{value[2]}\r", skipCondition: value.Length < 3),
                    str => Parser.IsLineValid(str, () => $"{value[3]}\r", skipCondition: value.Length < 4),
                    str => Parser.IsLineValid(str, () => $"{value[4]}\r", skipCondition: value.Length < 5));

                AssertStream.AssertOutput(
                    error!,
                    "200 OK",
                    TimeSpan.FromSeconds(30),
                    expectedCount: expectedHttpCalls++);
                Cosmos.ValidateRecods(value.Select(c => c.ToString()));

                reset.Set(); // init next iteration
                confirmation.WaitOne(); // next iteration is ready
            }

            cancellationTokenSource.Cancel();
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                // see comment above why 14 and not 12
                Assert.Equal(expected: 14M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 14M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_logs_are_read_when_updating_last_record()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            using var cancellationTokenSource = new CancellationTokenSource();
            using (var stream = File.Create(_testRunRawLog)) { /* close handle */ }

            var values = new int[][] { [1, 2, 3], [4], [5, 6] }
                .Select(i => i.Select(ii => Markup.CreateMarkeredLine(ii)).ToArray())
                .ToArray(); /* also represents expected output */
            var reset = new AutoResetEvent(false);
            var confirmation = new AutoResetEvent(false);
            var task = Task.Factory.StartNew(() =>
            {
                foreach (var attempt in values)
                {
                    var duration = Stopwatch.StartNew();
                    var attemptTag = string.Join(",", attempt);

                    File.AppendAllLines(_testRunRawLog, attempt);
                    confirmation.Set();
                    reset.WaitOne();
                }
                confirmation.Set(); // release waiting in last iteration
            },
            cancellationTokenSource.Token);
            confirmation.WaitOne(); // first iteration

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            int expectedHttpCalls = 1;
            foreach (var value in values)
            {
                AssertStream.AssertOutput(
                    output!,
                    timeout: TimeSpan.FromSeconds(30),
                    logTag: $"Iteration_{expectedHttpCalls}", // expectedHttpCalls represents iteration too
                    expectedCount: null,
                    throwIfFound: false,
                    testOutputHelper: _testOutputHelper,
                    str => Parser.IsLineValid(str, () => $"{value[0]}\r"),
                    str => Parser.IsLineValid(str, () => $"{value[1]}\r", skipCondition: value.Length < 2),
                    str => Parser.IsLineValid(str, () => $"{value[2]}\r", skipCondition: value.Length < 3));

                AssertStream.AssertOutput(
                    error!,
                    "200 OK",
                    TimeSpan.FromSeconds(30),
                    expectedCount: expectedHttpCalls++);
                Cosmos.ValidateRecods(value.Select(c => c.ToString()));

                reset.Set(); // init next iteration
                confirmation.WaitOne(); // next iteration is ready
            }

            cancellationTokenSource.Cancel();
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 6M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 6M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_logs_are_read_from_file_and_http_sources()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            var functionPortWithHttpSource = 7175;
            using var httpSourceFunctionProcess = Powershell.SpawnFunction(
                __originalAzureFunctionHttpSourceProjectDirectory,
                _stdError,
                functionPortWithHttpSource,
                "LogsSource: [GET]");

            FileSystem.RenameFile(__originalConfigWithHttpSource, _testRunConfig);

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: 2,
                throwIfFound: false,
                _testOutputHelper,
                str => str.Contains("HttpSource_value:"));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: null);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 2M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 2M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [SkippableFact]
        public async Task Ensure_logs_can_be_pushed_to_Datadog_via_sink()
        {
            if (Environment.GetEnvironmentVariable("DATADOG_API_KEY") == null)
            {
                throw new Xunit.SkipException("DATADOG_API_KEY must be specified");
            }

            // no cosmosdb call

            FileSystem.RenameFile(__originalConfigWithDataDogSink, _testRunConfig);

            var values = new[]
            {
                "datadog_1_" + Guid.NewGuid(),
                "datadog_2_" + Guid.NewGuid(),
                "datadog_3_" + Guid.NewGuid(),
                "datadog_4_" + Guid.NewGuid(),
                "datadog_5_" + Guid.NewGuid()
            };
            File.WriteAllText(_testRunRawLog, $@"{values[0]}
{values[1]}
{values[2]}
{values[3]}
{values[4]}
");

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: 1,
                throwIfFound: false,
                _testOutputHelper,
                str => str.Contains(values[4]));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: null);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 5M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 5M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "datadog").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [SkippableFact]
        public async Task Ensure_cosmos_records_are_transmitted_to_Datadog()
        {
            if (Environment.GetEnvironmentVariable("DATADOG_API_KEY") == null)
            {
                throw new Xunit.SkipException("DATADOG_API_KEY must be specified");
            }

            await Cosmos.PrepareCosmos(containerName: Cosmos.SourceContainerName); // clear test db
            var functionPortWithHttpSource = 7175;
            using var httpSourceFunctionProcess = Powershell.SpawnFunction(
                __originalAzureFunctionHttpSourceProjectDirectory,
                _stdError,
                functionPortWithHttpSource,
                "LogsSource: [GET]");

            FileSystem.RenameFile(__originalConfigWithHttpSourceAndDataDogSink, _testRunConfig);

            var values = new[]
            {
                (Id: 1, Message: "datadog_1_" + Guid.NewGuid()),
                (Id: 2, Message: "datadog_2_" + Guid.NewGuid()),
                (Id: 3, Message: "datadog_3_" + Guid.NewGuid()),
                (Id: 4, Message: "datadog_4_" + Guid.NewGuid()),
                (Id: 5, Message: "datadog_5_" + Guid.NewGuid())
            };
            await Cosmos.InsertRecords(values, containerName: Cosmos.SourceContainerName);

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(130),
                logTag: "mainAssert",
                expectedCount: 1,
                throwIfFound: false,
                _testOutputHelper,
                str => str.Contains(values[0].Message),
                str => str.Contains(values[1].Message),
                str => str.Contains(values[2].Message),
                str => str.Contains(values[3].Message),
                str => str.Contains(values[4].Message));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(30), expectedCount: 2);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 1, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 1, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "datadog").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_logs_are_read_from_file_created_beforehand_and_that_internal_logs_are_saved_through_second_config()
        {
            FileSystem.RenameFile(__originalInternalLogsConfig, _testRunConfig);

            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            var values = new[]
            {
                "1_" + Guid.NewGuid(),
                "2_" + Guid.NewGuid(),
                "3_" + Guid.NewGuid(),
                "4_" + Guid.NewGuid(),
                "5_" + Guid.NewGuid()
            };
            File.WriteAllText(_testRunRawLog, $@"{values[0]}
{values[1]}
{values[2]}
{values[3]}
{values[4]}
");

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[1]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[2]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[3]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[4]}\r"));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: 1);
            Cosmos.ValidateRecods(values);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 5M, result!.Sources!.Edges!.Single(i => i.Node!.ComponentId == "fileIn").Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 5M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "http").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
            Powershell.KillTool(_stdError, "vector");
            // No actual messages are expected in the logs
            var outputContent = File.ReadAllLines(Path.Combine(_testRunDirectory, "output", "output.json"));
            Assert.Contains(outputContent, (s) => s.Contains("Received HTTP request."));
        }

        [Fact]
        public async Task Ensure_logs_are_read_from_file_created_beforehand_without_duplicates()
        {
            FileSystem.RenameFile(__originalLogsConfigWithDedupeTransform, _testRunConfig);

            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            var values = new[]
            {
                "1_value",
                "2_value",
                "2_value",
                "2_value",
                "3_value"
            };
            File.WriteAllText(_testRunRawLog, $@"{values[0]}
{values[1]}
{values[2]}
{values[3]}
{values[4]}
");

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[1]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[2]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[3]}\r"),
                str => Parser.IsLineValid(str, () => $"{values[4]}\r"));

            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 5M, result!.Sources!.Edges!.Single(i => i.Node!.ComponentId == "fileIn").Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 3M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "console_print").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }

        [Fact]
        public async Task Ensure_multi_event_single_logs_is_unwrapped()
        {
            await Cosmos.PrepareCosmos(); // clear test db
            using var azureFunctionProcess = Powershell.SpawnFunction(__originalAzureFunctionProjectDirectory, _stdError, SinkHttpFunctionPort, waitUntil: "LogsTransmitter: [POST]");

            var functionPortWithHttpSource = 7175;
            using var httpSourceFunctionProcess = Powershell.SpawnFunction(
                __originalAzureFunctionHttpSourceProjectDirectory,
                _stdError,
                functionPortWithHttpSource,
                "LogsSource: [GET]");

            FileSystem.RenameFile(__originalLogsConfigWithHttpSourceAndUnwrapArrayOfMessages, _testRunConfig);

            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out var error,
                inWindow: false);

            AssertStream.AssertOutput(
                output!,
                timeout: TimeSpan.FromSeconds(30),
                logTag: "mainAssert",
                expectedCount: 2,
                throwIfFound: false,
                _testOutputHelper,
                str => str.Contains("HttpSource_value:"));

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: null);
            await GraphQLHelper.ValidateThroughApi((result) =>
            {
                Assert.Equal(expected: 1M, result!.Sources!.Edges!.Single().Node!.Metrics!.SentEventsTotal!.SentEventsTotal);
                Assert.Equal(expected: 2M, result!.Sinks!.Edges!.Single(i => i.Node!.ComponentId == "console_print").Node!.Metrics!.ReceivedEventsTotal!.ReceivedEventsTotal);
            });
        }
    }
}