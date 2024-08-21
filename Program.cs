using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;

namespace SqlBackupScheduler
{
    class Program
    {
        static void Main()
        {
            // Load configuration from appsettings.json
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            // Retrieve configuration values
            string connectionString = configuration["ConnectionString"];
            string backupLocation = configuration["BackupLocation"];
            string[] databases = configuration.GetSection("Databases").Get<string[]>();
            int backupRetentionDays = int.Parse(configuration["BackupRetentionDays"]);

            // Backup the databases
            BackupDatabases(connectionString, backupLocation, databases);

            // Clean up old backups
            CleanupOldBackups(backupLocation, backupRetentionDays);
        }

        static void BackupDatabases(string connectionString, string backupLocation, string[] databases)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                foreach (var database in databases)
                {
                    // Create a timestamp for the backup file name
                    string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                    string backupFileName = Path.Combine(backupLocation, $"{database}_backup_{timestamp}.bak");

                    // Create the SQL command for backing up the database
                    string backupQuery = $"BACKUP DATABASE [{database}] TO DISK = '{backupFileName}'";

                    try
                    {
                        using (SqlCommand command = new SqlCommand(backupQuery, connection))
                        {
                            // Execute the backup command
                            command.ExecuteNonQuery();
                            Console.WriteLine($"Database '{database}' backed up successfully to '{backupFileName}'.");
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any errors that occur during the backup
                        Console.WriteLine($"Error backing up database '{database}': {ex.Message}");
                    }
                }
            }
        }

        static void CleanupOldBackups(string backupLocation, int retentionDays)
        {
            try
            {
                // Get all .bak files in the backup directory
                var backupFiles = Directory.GetFiles(backupLocation, "*.bak");

                foreach (var file in backupFiles)
                {
                    // Get the file's creation time
                    var creationTime = File.GetCreationTime(file);

                    // If the file is older than the retention period, delete it
                    if (creationTime < DateTime.Now.AddDays(-retentionDays))
                    {
                        File.Delete(file);
                        Console.WriteLine($"Deleted old backup file: {file}");
                    }
                }
            }
            catch (Exception ex)
            {
                // Handle any errors that occur during the cleanup
                Console.WriteLine($"Error during cleanup of old backups: {ex.Message}");
            }
        }
    }

}
