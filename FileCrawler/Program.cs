using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace FileCrawler
{
    internal sealed class Document
    {
        private const string FileExtension = ".zip";

        public Guid Id { get; private set; }
        public string Filename { get; private set; }
        public string Location { get; private set; }
        public DateTime CreatedOn { get; private set; }
        public int FileNumber { get; private set; }
        public string FilenameAndPath { get; private set; }

        public Document(Guid id, string location, DateTime createdOn, int fileNumber)
        {
            Id = id;
            Filename = id.ToString("N") + FileExtension;
                // The formatting of ToString can be changed to match the filenames
            Location = location;
            CreatedOn = createdOn;
            FileNumber = fileNumber;
            FilenameAndPath = Path.Combine(Location, Filename);
        }
    }


    internal static class Program
    {
        public static void Main()
        {
            new Runner().Run();
        }
    }

    internal sealed class Runner
    {
        private const string RootDir = @"\\server";

        private static readonly string ConnString = string.Empty;

        private const int BatchCount = 50;

        private const string SelectFileInformationSql =
            @"select top 1 d.UID as id, d.CreatedDate as createdOn, d.Location as location, m.number as fileNumber from master m
                                    inner join documentation_attachments da
                                        on m.number = da.accountid
                                    inner join documentation d
                                        on da.documentid = d.uid
                                    where m.qlevel in (998,999)
                                    and d.location is not null
                                    and uid not in (select documentid from DocumentationIssues)";

        private const string UpdateDocumentLocationSql =
            "update documentation set location = @newLocation where uid = @id";

        private const string LogIssueSql =
            @"insert into documentationissues (documentid, oldLocation, newLocation, errorMessage, dateAdded, number)
                                values (@id, @prevLocation, @newLocation, @msg, GETDATE(), @number)";

        private static int _fileCount;
        private static int _errorCount;

        public void Run()
        {
            OpenDatabaseAndProcessResults();
        }

        private void OpenDatabaseAndProcessResults()
        {
            Console.Clear();
            // reset variables
            using (var connection = new SqlConnection(ConnString))
            {
                ReadFromDatabaseAndProcessRows(connection);
            }
        }

        private void ReadFromDatabaseAndProcessRows(IDbConnection connection)
        {
            connection.Open();
            // Run through

            var files = connection.Query<Document>(SelectFileInformationSql).ToList();

            var startFile = 0;
            var tasks = new List<Task>();
            while (startFile <= files.Count())
            {
                var batch = files.Skip(startFile).Take(BatchCount);

                tasks.Add(new Task(() => ProcessRows(batch)));

                startFile += BatchCount;
            }

            Task.WaitAll(tasks.ToArray());
        }

        private void ProcessRows(IEnumerable<Document> documents)
        {
            if (!CheckAndReportIfProcessHasRunOverTime()) return;

            Console.WriteLine("Processing data...");

            foreach (var document in documents)
            {
                ProcessDocument(document);
            }
        }

        private async void ProcessDocument(Document document)
        {
            Console.WriteLine("Current File #: {0}", _fileCount);
            Console.WriteLine("Working on document {0}", document.Id);

            var fileinfo = new FileInfo(document.FilenameAndPath);

            if (!CheckAndLogIfFileExists(fileinfo, document))
            {
                _errorCount++;
                return;
            }

            // file exists begin process to create new folders
            var fileYear = "DOCS" + document.CreatedOn.Year.ToString(CultureInfo.InvariantCulture);
            var fileMonth = document.CreatedOn.ToString("MMM");

            Console.WriteLine("File Exists, checking to make sure the directories needed exist.");

            if (!CheckAndReportIfRootYearFolderExists(fileYear, document))
            {
                _errorCount++;
                return;
            }

            if (!Directory.Exists(Path.Combine(RootDir, fileYear, fileMonth)))
            {
                // Create the month folder
                Directory.CreateDirectory(Path.Combine(RootDir, fileYear, fileMonth));
                Console.WriteLine("The month folder did not exist, created {0} folder",
                                  fileMonth);
            }

            // Call method to update location in database and move tile
            await UpdateDocumentAsync(document.Id, Path.Combine(RootDir, fileYear, fileMonth));
            fileinfo.MoveTo(Path.Combine(RootDir, fileYear, fileMonth, document.Filename));
            //File.Move(Path.Combine(location, fileName), Path.Combine(rootDir, fileYear, fileMonth, fileName));

            LogError(document.Id, document.Location, document.FilenameAndPath, "SUCCESS", document.FileNumber);
            //runLoop = false;
            Console.WriteLine(
                "Successfully moved and logged the file, checking for more files.");
            _fileCount++;
        }

        private bool CheckAndReportIfRootYearFolderExists(string fileYear, Document document)
        {
            if (Directory.Exists(Path.Combine(RootDir, fileYear))) return true;

            // Error, cannot create root level network share folder.  Log to Database.
            // Error Root Level Folder Missing
            // Directory.CreateDirectory(Path.Combine(rootDir,fileYear));
            // Log error to DocumentationIssues
            LogError(document.Id, document.Location, document.FilenameAndPath,
                     "Could not create root folder, log to skip this file", document.FileNumber);
            return false;
        }

        private bool CheckAndLogIfFileExists(FileSystemInfo fileinfo, Document document)
        {
            if (fileinfo.Exists) return true;

            // Log error to DocumentationIssues
            const string msg = "This file does not exist";
            LogError(document.Id, document.Location, "This file does not exist", msg, document.FileNumber);
            Console.WriteLine(
                "This file did not exist, logged it to the database and moving on to the next file.");

            return false;
        }

        private bool CheckAndReportIfProcessHasRunOverTime()
        {
            var currentDate = DateTime.Now;
            if (currentDate.DayOfWeek == DayOfWeek.Monday && currentDate.Hour >= 20)
            {
                Console.WriteLine("Processed {0} files successfully.", _fileCount);
                Console.WriteLine("Did not process {0} files successfully.", _errorCount);
                Console.WriteLine(
                    "The time set for this process to end has been reached.  Program exited at {0}",
                    DateTime.Now);
                Console.ReadLine();
                Console.WriteLine("Exiting program.");
                return false;
            }

            return true;
        }

        private bool CheckAndReportIfReaderHasRows(DbDataReader reader)
        {
            if (reader.HasRows) return true;


            Console.WriteLine("Processed {0} files successfully.", _fileCount);
            Console.WriteLine("Did not process {0} files successfully.", _errorCount);
            Console.WriteLine("No more files were found with the current query");
            Console.ReadLine();
            Console.WriteLine("Exiting program.");

            return false;
        }

        private Task UpdateDocumentAsync(Guid id, string newLocation)
        {
            using (var connection = new SqlConnection(ConnString))
            {
                connection.Open();


                using (var command = new SqlCommand(UpdateDocumentLocationSql, connection))
                {
                    command.Parameters.Add(new SqlParameter("id", id));
                    command.Parameters.Add(new SqlParameter("newLocation", newLocation));

                    return command.ExecuteNonQueryAsync();
                }
            }
        }

        private void LogError(Guid id, string prevLocation, string newLocation, string msg, int number)
        {
            if (newLocation == null)
            {
                newLocation = "no new location";
            }

            using (var connection = new SqlConnection(ConnString))
            {
                connection.Open();

                using (var command = new SqlCommand(LogIssueSql, connection))
                {
                    command.Parameters.Add(new SqlParameter("id", id));
                    command.Parameters.Add(new SqlParameter("prevLocation", prevLocation));
                    command.Parameters.Add(new SqlParameter("newLocation", newLocation));
                    command.Parameters.Add(new SqlParameter("msg", msg));
                    command.Parameters.Add(new SqlParameter("number", number));

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}