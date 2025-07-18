using Microsoft.Extensions.Configuration;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.Runtime;
using System.Data.SqlClient;
using System.Net;

namespace SqlBackupScheduler
{
    class Program
    {
        static void Main()
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            string connectionString = configuration["ConnectionString"];
            string backupLocation = configuration["BackupLocation"];
            string[] databases = configuration.GetSection("Databases").Get<string[]>();
            int backupRetentionDays = int.Parse(configuration["BackupRetentionDays"]);

            // FTP
            string ftpServer = configuration["FtpServer"];
            string ftpUsername = configuration["FtpUsername"];
            string ftpPassword = configuration["FtpPassword"];
            string ftpUploadPath = configuration["FtpUploadPath"];

            // S3
            bool useS3 = bool.TryParse(configuration["UseS3Upload"], out var useS3Upload) && useS3Upload;
            var s3Settings = configuration.GetSection("S3Settings");

            string[] backups = BackupDatabases(connectionString, backupLocation, databases);

            if (useS3)
            {
                UploadBackupsToS3(
                    backups,
                    s3Settings["Endpoint"],
                    s3Settings["AccessKey"],
                    s3Settings["SecretKey"],
                    s3Settings["BucketName"],
                    s3Settings["Region"]
                );
            }
            else
            {
                UploadBackupsToFtp(backups, ftpServer, ftpUsername, ftpPassword, ftpUploadPath);
            }

            CleanupOldBackups(backupLocation, backupRetentionDays);
        }

        static string[] BackupDatabases(string connectionString, string backupLocation, string[] databases)
        {
            List<string> newBackups = new List<string>();
            using SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();

            foreach (var database in databases)
            {
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string backupFileName = Path.Combine(backupLocation, $"{database}_backup_{timestamp}.bak");
                string backupQuery = $"BACKUP DATABASE [{database}] TO DISK = '{backupFileName}'";

                try
                {
                    using SqlCommand command = new SqlCommand(backupQuery, connection);
                    command.CommandTimeout = 0;
                    command.ExecuteNonQuery();
                    Console.WriteLine($"Database '{database}' backed up to '{backupFileName}'.");
                    newBackups.Add(backupFileName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error backing up '{database}': {ex.Message}");
                }
            }
            return newBackups.ToArray();
        }

        static void UploadBackupsToFtp(string[] backupFiles, string ftpServer, string ftpUsername, string ftpPassword, string ftpUploadPath)
        {
            foreach (var file in backupFiles)
            {
                try
                {
                    string fileName = Path.GetFileName(file);
                    string ftpUri = $"{ftpServer}{ftpUploadPath}{fileName}";
                    FtpWebRequest request = WebRequest.Create(ftpUri) as FtpWebRequest;
                    request.Method = WebRequestMethods.Ftp.UploadFile;
                    request.Credentials = new NetworkCredential(ftpUsername, ftpPassword);

                    byte[] fileContents = File.ReadAllBytes(file);
                    using Stream requestStream = request.GetRequestStream();
                    requestStream.Write(fileContents, 0, fileContents.Length);

                    using FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                    Console.WriteLine($"Uploaded '{fileName}' to FTP. Status: {response.StatusDescription}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"FTP upload error: {ex.Message}");
                }
            }
        }

        static void UploadBackupsToS3(string[] backupFiles, string endpoint, string accessKey, string secretKey, string bucketName, string region)
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            var config = new AmazonS3Config
            {
                ServiceURL = endpoint,
                ForcePathStyle = true,
                RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region)
            };

            using var client = new AmazonS3Client(credentials, config);
            var transferUtility = new TransferUtility(client);

            foreach (var file in backupFiles)
            {
                try
                {
                    transferUtility.Upload(file, bucketName);
                    Console.WriteLine($"Uploaded '{Path.GetFileName(file)}' to S3 bucket '{bucketName}'.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"S3 upload error: {ex.Message}");
                }
            }
        }

        static void CleanupOldBackups(string backupLocation, int retentionDays)
        {
            try
            {
                var backupFiles = Directory.GetFiles(backupLocation, "*.bak");
                foreach (var file in backupFiles)
                {
                    var creationTime = File.GetCreationTime(file);
                    if (creationTime < DateTime.Now.AddDays(-retentionDays))
                    {
                        File.Delete(file);
                        Console.WriteLine($"Deleted old backup: {file}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Cleanup error: {ex.Message}");
            }
        }
    }
}
