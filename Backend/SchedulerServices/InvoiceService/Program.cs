using Serilog;
namespace InvoiceService
{
    public class Program
    {
        public static void Main(string[] args)
        {


            // Build configuration to read from appsettings.json
            //var config = new ConfigurationBuilder()
            //    .SetBasePath(Directory.GetCurrentDirectory())
            //    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            //    .Build();
            //// Get the log file path from configuration
            //var logFilePath = config["LogFilePath"];
            //if (string.IsNullOrEmpty(logFilePath))
            //{
            //    throw new InvalidOperationException("Log file path is not defined in appsettings.json");
            //}

            //// Set up Serilog configuration
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()  // Log to the console (optional)
                .WriteTo.File("logs/invoiceWorker-log.txt", rollingInterval: RollingInterval.Day)  // Log to a file (rolling by day)
                .CreateLogger();

            // Set up Serilog from appsettings.json
            //Log.Logger = new LoggerConfiguration()
            //.ReadFrom.Configuration(config) // Read other configurations
            //.WriteTo.File(Path.Combine(logFilePath, "Invoice.log"), rollingInterval: RollingInterval.Day, retainedFileCountLimit: 7)
            //.CreateLogger();

            //Log.Logger = new LoggerConfiguration()
            //    .ReadFrom.Configuration(new ConfigurationBuilder()
            //        .SetBasePath(Directory.GetCurrentDirectory())
            //        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            //        .Build())
            //    .CreateLogger();



            var builder = Host.CreateApplicationBuilder(args);


            builder.Services.AddWindowsService();
            builder.Services.AddHostedService<Worker>();





            // Integrate Serilog into the logging system
            builder.Services.AddLogging(builder =>
            {
                builder.AddSerilog();  // Adds Serilog to the logging pipeline
            });

            try
            {
                var host = builder.Build();
                host.Run();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "An unhandled exception occurred during application startup.");
            }
            finally
            {
                Log.CloseAndFlush();
            }

        }
    }
}