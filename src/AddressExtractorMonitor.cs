using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using MyAddressExtractor.Objects;
using MyAddressExtractor.Objects.Performance;

namespace MyAddressExtractor {
    public class AddressExtractorMonitor : IAsyncDisposable {
        private readonly Runtime Runtime;
        private Config Config => this.Runtime.Config;

        private readonly Channel<Line> LineChannel;
        private ChannelReader<Line> LineReader => this.LineChannel.Reader;
        private ChannelWriter<Line> LineWriter => this.LineChannel.Writer;

        private readonly Channel<LineResult> EmailChannel;
        private ChannelReader<LineResult> EmailReader => this.EmailChannel.Reader;
        private ChannelWriter<LineResult> EmailWriter => this.EmailChannel.Writer;

        private readonly IList<Task> LineTasks = new List<Task>();
        private readonly Task EmailTask;

        private readonly AddressExtractor Extractor;
        private readonly IPerformanceStack Stack;

        protected readonly IDictionary<string, Count> Files = new ConcurrentDictionary<string, Count>();
        protected readonly ISet<string> Addresses = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // ReadLine count
        protected int Lines => this.LineCounter;
        private int LineCounter;

        protected readonly Stopwatch Stopwatch = Stopwatch.StartNew();
        private readonly Timer Timer;

        public AddressExtractorMonitor(
            Runtime runtime,
            IPerformanceStack stack
        ): this(runtime, stack, TimeSpan.FromMinutes(1)) {}

        public AddressExtractorMonitor(
            Runtime runtime,
            IPerformanceStack stack,
            TimeSpan iterate
        ) {
            this.Runtime = runtime;
            this.LineChannel = this.Config.CreateSingleWriterChannel<Line>();
            this.EmailChannel = this.Config.CreateSingleReaderChannel<LineResult>(1000);
            this.Extractor = new AddressExtractor(runtime);
            this.Stack = stack;
            this.Timer = new Timer(_ => this.Log(), null, iterate, iterate);

            for (int i = 0; i < this.Config.Threads; i++)
            {
                var task = Task.Run(() => this.ReadLinesAsync(this.Runtime.CancellationToken));
                this.LineTasks.Add(task);
            }

            this.EmailTask = Task.Run(() => this.ReadEmailsAsync(this.Runtime.CancellationToken));
        }

        private async Task ReadLinesAsync(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                Line line = default;

                try
                {
                    // Tasks run forever
                    while (!cancellation.IsCancellationRequested)
                    {
                        // Get a line from the Channel
                        line = await this.LineReader.ReadAsync(cancellation);

                        // Check for pauses
                        await this.Runtime.AwaitContinuationAsync(cancellation);

                        // Extract addresses from the line
                        await foreach(var email in this.Extractor.ExtractAddressesAsync(this.Stack, line.Value, cancellation))
                        {
                            await this.EmailWriter.WriteAsync(new LineResult {
                                Address = email,
                                Counter = line.Counter
                            }, cancellation);
                        }

                        Interlocked.Increment(ref this.LineCounter);

                        // Disabled currently, alternating log messages has oddities
                        /*if (!this.Config.Quiet && this.Lines % 25000 is 0)
                            Output.Write($"Checked {this.Lines:n0} lines, found {line.Counter.Value:n0} emails");*/
                    }
                } catch (ChannelClosedException) {
                    break; // Break the Task when Channel closed
                } catch (TaskCanceledException) {
                    break; // Break the Task when Tasks are cancelled
                } catch (Exception ex) {
                    if (this.Config.Debug)
                        Output.Exception(new Exception($"An error occurred while parsing '{line.File}'L{line.Number}:", ex));
                    else
                        Output.Error($"An error occurred while parsing '{line.File}'L{line.Number}: {ex.Message}");

                    if (!await this.Runtime.WaitOnExceptionAsync(cancellation))
                        break;
                }
            }
        }

        private async Task ReadEmailsAsync(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                LineResult address = default;

                try
                {
                    // Task runs forever
                    while (!cancellation.IsCancellationRequested)
                    {
                        // Get an address from the channel
                        address = await this.EmailReader.ReadAsync(cancellation);

                        // Check for pauses
                        await this.Runtime.AwaitContinuationAsync(cancellation);

                        if (
                            !string.IsNullOrEmpty(address.Address)
                            && this.Addresses.Add(address.Address)
                        ) address.Counter.Increment();
                    }
                } catch (ChannelClosedException) {
                    break; // Break the Task when Channel closed
                } catch (TaskCanceledException) {
                    break; // Break the Task when Tasks are cancelled
                } catch (Exception ex) {
                    if (this.Config.Debug)
                        Output.Exception(new Exception($"An error occurred while storing '{address.Address}':", ex));
                    else
                        Output.Error($"An error occurred while storing '{address.Address}': {ex.Message}");

                    if (!await this.Runtime.WaitOnExceptionAsync(cancellation))
                        break;
                }
            }
        }

        public async ValueTask RunAsync(FileInfo file, CancellationToken cancellation = default)
        {
            using (var stack = this.Stack.CreateStack("Read file"))
            {
                var lines = 0;
                var count = new Count();

                this.Files.Add(file.FullName, count);

                var parser = this.Runtime.GetExtension(file);
                await using (var reader = parser.GetReader(file.FullName))
                {
                    // Await any 'continue' prompts
                    await this.Runtime.AwaitContinuationAsync(cancellation);

                    Output.Write($"Reading \"{file.FullName}\" [{ByteExtensions.Format(file.Length)}]");
                    await foreach(var line in reader.ReadLineAsync(cancellation))
                    {
                        stack.Step("Read line");
                        if (line is not null)
                        {
                            await this.LineWriter.WriteAsync(new Line {
                                File = file.FullName,
                                Value = line,
                                Counter = count,
                                Number = ++lines
                            }, cancellation);

                            if (!this.Config.Quiet && lines % 25000 is 0)
                                Output.Write($"Read {lines:n0} lines from \"{file.Name}\"");
                        }
                    }
                }
            }
        }

        public virtual void Log()
        {
            Output.Write($"Extraction time: {this.Stopwatch.Format()}");
            Output.Write($"Addresses extracted: {this.Addresses.Count:n0}");
            long rate = (long)(this.Lines / (this.Stopwatch.ElapsedMilliseconds / 1000.0));
            Output.Write($"Read lines total: {this.Lines:n0}");
            Output.Write($"Read lines rate: {rate:n0}/s\n");

            this.Stack.Log();
        }

        internal async ValueTask AwaitCompletionAsync()
        {
            this.LineWriter.Complete();

            await Task.WhenAll(this.LineTasks);
            this.EmailWriter.Complete();
            await Task.WhenAll(new[] { this.EmailTask });

            await this.LineReader.Completion;
        }

        internal async ValueTask SaveAsync(CancellationToken cancellation = default)
        {
            string output = this.Config.OutputFilePath;
            string report = this.Config.ReportFilePath;
            if (!string.IsNullOrWhiteSpace(output))
            {
                await File.WriteAllLinesAsync(
                    output,
                    this.Addresses.Select(address => address.ToLowerInvariant())
                        .OrderBy(address => address, StringComparer.OrdinalIgnoreCase),
                    cancellation
                );
            }

            if (!string.IsNullOrWhiteSpace(report))
            {
                var reportContent = new StringBuilder("Unique addresses per file:\n");

                foreach ((var file, var count) in this.Files)
                {
                    reportContent.AppendLine($"{file}: {count}");
                }

                await File.WriteAllTextAsync(report, reportContent.ToString(), cancellation);
            }
        }

        public async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);

            await this.Timer.DisposeAsync();
            this.Stopwatch.Stop();
        }
    }
}
