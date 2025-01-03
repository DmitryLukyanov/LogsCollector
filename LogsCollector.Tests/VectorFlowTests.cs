using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit.Abstractions;

namespace LogsCollector.Tests
{
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
        private static readonly string __originalProjectDirectory = Path.GetFullPath(
            Path.Combine(
                __testDirectory,
                "..",
                "LogsCollector"));
        private static readonly string __originalConfigDirectory = Path.GetFullPath(
            Path.Combine(
                __originalProjectDirectory,
                "config"));
        private static readonly string __originalLaunchPs1Path = Path.Combine(__originalProjectDirectory, "Launch.ps1");
        #endregion

        private readonly Process _azureFunctionProcess;
        private readonly int _port;
        private readonly ConcurrentQueue<string> _stdError;
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly string _testRunConfig;
        private readonly string _testRunDirectory;
        private readonly string _testRunLaunchPs1;
        private readonly string _testRunLogDirectory;
        private readonly string _testRunRawLog;

        public VectorFlowTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            _stdError = new();
            Powershell.EnsureVectorIsAvailable(_stdError);
            Cosmos.PrepareCosmos(); // clear test db

            _port = 7245;
            _azureFunctionProcess = Powershell.SpawnFunction(_stdError, _port);

            /* 
             * sometimes this process is not killed since previous run and this leads to this error:
             * - Only one usage of each socket address 
             */
            Powershell.KillVector(_stdError);

            // prepare test
            _testRunDirectory = FileSystem.CreateDirectory(__testDirectory, $"run_{DateTime.UtcNow:MMddyyyyHHmmss}");
            _testRunLogDirectory = FileSystem.CreateDirectory(_testRunDirectory, "logs");
            _testRunRawLog = Path.Combine(_testRunLogDirectory, "raw.log");
            _testRunLaunchPs1 = FileSystem.CopyFile(__originalLaunchPs1Path, _testRunDirectory, Path.GetFileName(__originalLaunchPs1Path));

            var testRunConfigDirectory = FileSystem.CopyDirectory(
                original: __originalConfigDirectory,
                _testRunDirectory, Path.GetFileName(__originalConfigDirectory));
            _testRunConfig = Path.Combine(testRunConfigDirectory, "vector.yaml");
            if (!File.Exists(_testRunConfig))
            {
                throw new InvalidOperationException("The test run config file doesn't exist");
            }
        }

        public void Dispose()
        {
            _azureFunctionProcess?.Close();
            _azureFunctionProcess?.Dispose();
            if (_stdError != null && !_stdError.IsEmpty)
            {
                // TODO: merge below checks

                // specific check
                AssertStream.AssertOutput(
                    _stdError!,
                    "Only one usage of each socket address",
                    TimeSpan.FromSeconds(10),
                    throwIfFound: true,
                    testOutputHelper: _testOutputHelper);

                // just check on unhandled ERROR
                AssertStream.AssertOutput(
                    _stdError!,
                    " ERROR vector",
                    TimeSpan.FromSeconds(10),
                    throwIfFound: true,
                    testOutputHelper: _testOutputHelper);
            }
        }

        [Fact]
        public void Ensure_config_is_valid()
        {
            using var process = Powershell.RunPs1Script(
                @$"-ExecutionPolicy Bypass -File ""{_testRunLaunchPs1}""",
                workingDirectory: _testRunDirectory,
                errorStdOutput: _stdError,
                out var output,
                out _);

            AssertStream.AssertOutput(output!, "Validated", timeout: TimeSpan.FromSeconds(10));
        }

        [Fact]
        public void Ensure_logs_are_read_from_file_created_beforehand()
        {
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
        }

        [Fact]
        public void Ensure_logs_are_read_when_batch_size_is_less_than_number_of_lines_in_the_file()
        {
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
                timeout: TimeSpan.FromSeconds(45),
                logTag: "mainAssert",
                expectedCount: null,
                throwIfFound: false,
                testOutputHelper: _testOutputHelper,
                str => Parser.IsLineValid(str, () => $"{values[0]}\r"), // first element
                str => Parser.IsLineValid(str, () => $"{values[values.Length / 2]}\r"), // something in the middle
                str => Parser.IsLineValid(str, () => $"{values[^1]}\r")); // last element

            AssertStream.AssertOutput(error!, "200 OK", TimeSpan.FromSeconds(10), expectedCount: 6); // 6 different http calls
            Cosmos.ValidateRecods(values);
        }

        [Theory]
        [InlineData("\\n", "\n")]
        [InlineData("\\r\\n", "\r\n")]
        public void Ensure_logs_are_read_when_custom_delimiter_is_specified(string delimiter, string effectiveDelimiter)
        {
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
        }

        [Fact]
        public void Ensure_logs_are_read_when_multiline_logs_is_specified()
        {
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
        }

        [Fact]
        public void Ensure_logs_are_read_when_updating_first_line_read_all_lines_again()
        {
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
            }, cancellationTokenSource.Token);
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
        }

        [Fact]
        public void Ensure_logs_are_read_when_updating_last_record()
        {
            using (var stream = File.Create(_testRunRawLog)) { /* close handle */ }

            var values = new int[][] { [1, 2, 3], [4], [5, 6] }
                .Select(i => i.Select(ii => Markup.CreateMarkeredLine(ii)).ToArray())
                .ToArray(); /* also represents expected output */
            var reset = new AutoResetEvent(false);
            var confirmation = new AutoResetEvent(false);
            var task = Task.Factory.StartNew(async () =>
            {
                await Task.Yield();
                foreach (var attempt in values)
                {
                    var duration = Stopwatch.StartNew();
                    var attemptTag = string.Join(",", attempt);

                    File.AppendAllLines(_testRunRawLog, attempt);
                    confirmation.Set();
                    reset.WaitOne();
                }
                confirmation.Set(); // release waiting in last iteration
            });
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
        }
    }
}