using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using Serilog.Events;
using System.CommandLine;
using System.CommandLine.Binding;
using System.Text;
using System.Text.Unicode;

using var log = new LoggerConfiguration()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var command = new RootCommand("Rabbitmq task runner spike solution");

var rabbitOption = new Option<string>(new[] { "-h", "--host" }, () => "localhost", "address of rabbitmq");

command.AddGlobalOption(rabbitOption);

var runnerCommand = new Command("runner", "starts a new runner");


runnerCommand.SetHandler<string>(ExecuteRunner, rabbitOption);

command.AddCommand(runnerCommand);
var runCommand = new Command("run", "runs a job");

runCommand.AddOption(new Option<string?>(new[] { "-p", "--pool" }, "name of the pool (default=none)"));
runCommand.AddOption(new Option<string?>(new[] { "-s", "--synchronization-queue" }, "name of the synchronization queue"));
runCommand.AddOption(new Option<int>(new[] { "-r", "--retries" }, () => 3, "number of retries after failure"));
runCommand.AddOption(new Option<int>("--retry-interval", () => 1000, "time (in ms) to wait before retrying a job"));
runCommand.AddOption(new Option<int>("--count", () => 1, "number of tasks to execute"));
runCommand.AddOption(new Option<double>("--failure-rate", () => 0, "failure rate (1.0 = 100%) of tasks"));

runCommand.SetHandler<string, string?, string?, int, int, int, double>(RunTask, new[] { rabbitOption }.Concat(runCommand.Options).Cast<IValueDescriptor>().ToArray());
command.AddCommand(runCommand);



await command.InvokeAsync(args);


void ExecuteRunner(string host)
{
    log.Information("Starting console task runner for rabbitmq: {host}", host);
    log.Information("Press <Ctrl+C> to end");

    var factory = new ConnectionFactory();
    factory.HostName = host;

    using var cts = new CancellationTokenSource();
    using var conn = factory.CreateConnection();
    using var channel = conn.CreateModel();

    Console.TreatControlCAsInput = false;
    Console.CancelKeyPress += (o, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    channel.QueueDeclare("default", true, false, false);

    var consumer = new EventingBasicConsumer(channel);

    consumer.Received += (o, e) =>
    {
        var taskName = Encoding.UTF8.GetString(e.Body.Span);

        log.Information("Starting task {taskName} from queue {queueName}", taskName, e.RoutingKey);

        Thread.Sleep(1000);

        var failureRate = e.BasicProperties.Headers.TryGetValue("Failure-Rate", out var rate) ? (double)rate : 0.0;

        var random = new Random();
        if (random.NextDouble() <= failureRate)
        {
            log.Warning("Task {taskName} failed", taskName);
            channel.BasicNack(e.DeliveryTag, false, false);

            //TODO: requeue after failure

        }
        else
        {
            log.Information("Task {taskName} succeeded", taskName);
            channel.BasicAck(e.DeliveryTag, false);
        }
    };

    //TODO: mechanism to subscribe to other pools/synchornization queues
    channel.BasicConsume("default", false, consumer);

    cts.Token.WaitHandle.WaitOne();

    log.Information("Bye");

}

void RunTask(string host, string? pool, string? synchronizationQueue, int retries, int retryInterval, int taskCount, double failureRate)
{
    var factory = new ConnectionFactory();
    factory.HostName = host;

    using var conn = factory.CreateConnection();
    using var channel = conn.CreateModel();
    channel.BasicQos(0, 1, true);

    pool ??= "default";

    var queueName = string.IsNullOrEmpty(synchronizationQueue) ? pool : $"{pool}.{synchronizationQueue}";

    var result = channel.QueueDeclare(queueName, true, false, false);

    var metadata = channel.CreateBasicProperties();

    metadata.ContentType = "text/plain";
    metadata.DeliveryMode = 2;
    metadata.Headers = new Dictionary<string, object> { { "Failure-Rate", failureRate } };

    for (int i = 0; i < taskCount; i++)
    {
        var taskName = "Task " + (i + 1);
        var payload = Encoding.UTF8.GetBytes(taskName);
        log.Information("Sending task {task} to queue {queueName}...", taskName, queueName);


        channel.BasicPublish(string.Empty, queueName, true, metadata, payload);
    }

    conn.Close();
}
