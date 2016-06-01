namespace DocumentDBBenchmark
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.ComponentModel;
    using System.Configuration;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.RegularExpressions;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Documents.Partitioning;
    using Newtonsoft;
    using Newtonsoft.Json;
    using System.Text;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// This sample demonstrates how to achieve high performance writes using DocumentDB.
    /// </summary>
    public sealed class Program
    {
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string DataCollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly string MetricCollectionName = ConfigurationManager.AppSettings["MetricCollectionName"];
        private static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);
        public static StoredProcedure sproc;

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway, ConnectionProtocol = Protocol.Https, RequestTimeout = new TimeSpan(1, 0, 0) };

        private static readonly int TaskCount = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
        private static readonly int DefaultConnectionLimit = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
        private static readonly string InstanceId = Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;
        private const int MinThreadPoolSize = 100;

        private static int pendingTaskCount,ii=100000,p=100;
        private static long documentsInserted;
        private static ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();
        private static DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client)
        {
            client = client;
        }

        /// <summary>
        /// Main method for the sample.
        /// </summary>
        /// <param name="args">command line arguments.</param>
        public static void Main(string[] args)
        {
            
            ServicePointManager.UseNagleAlgorithm = true;
            ServicePointManager.Expect100Continue = true;
            ServicePointManager.DefaultConnectionLimit = DefaultConnectionLimit;
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            string endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            string authKey = ConfigurationManager.AppSettings["AuthorizationKey"];
           
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Endpoint: {0}", endpoint);
            Console.WriteLine("Collection : {0}.{1} at {2} request units per second", DatabaseName, DataCollectionName, ConfigurationManager.AppSettings["CollectionThroughput"]);
            Console.WriteLine("Document Template*: {0}", ConfigurationManager.AppSettings["DocumentTemplateFile"]);
            Console.WriteLine("Degree of parallelism*: {0}", TaskCount);
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine();

            Console.WriteLine("DocumentDBBenchmark starting...");
           

            try
            {
                using (client = new DocumentClient(
                    new Uri(endpoint),
                    authKey,
                    ConnectionPolicy))
                {
                    var program = new Program(client);
                    program.RunAsync().Wait();
                    Console.WriteLine("DocumentDBBenchmark completed successfully.");
                }
            }

#if !DEBUG
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine("Samples failed with exception:{0}", e);
            }
#endif

            finally
            {
            }
        }

        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        private async Task RunAsync()
        {
            
            DocumentCollection dataCollection = GetCollectionIfExists(DatabaseName, DataCollectionName);

            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]) || dataCollection == null)
            {
                Database databasee = GetDatabaseIfExists(DatabaseName);
                if (databasee != null)
                {
                    await client.DeleteDatabaseAsync(databasee.SelfLink);
                }

                Console.WriteLine("Creating database {0}", DatabaseName);
                databasee = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                Console.WriteLine("Creating collection {0} with {1} RU/s", DataCollectionName, CollectionThroughput);
                dataCollection = await this.CreatePartitionedCollectionAsync(DatabaseName, DataCollectionName);
            }
            else
            {
                OfferV2 offer = (OfferV2)client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                int throughput = offer.Content.OfferThroughput;
                Console.WriteLine("Found collection {0} with {1} RU/s", DataCollectionName, CollectionThroughput);
            }
            Database database = await GetNewDatabaseAsync(DatabaseName);
            //Database database = client.CreateDatabaseQuery().Where(db => db.Id == DatabaseName).ToArray().FirstOrDefault();
            //Get, or Create, the Document Collection
            DocumentCollection collection = await CreateCollectionAsync(database.SelfLink, DataCollectionName);
            // DocumentCollection collection = client.CreateDocumentCollectionQuery(database.SelfLink)
            //   .Where(c => c.Id == DataCollectionName).AsEnumerable().FirstOrDefault();
            string body = File.ReadAllText(@".\JS\BulkImport.js");
            sproc = new StoredProcedure
            {
                Id = "BulkImport",
                Body = body
            };

            await TryDeleteStoredProcedure(collection.SelfLink, sproc.Id);
            sproc = await client.CreateStoredProcedureAsync(collection.SelfLink, sproc);
            DocumentCollection metricCollection = GetCollectionIfExists(DatabaseName, MetricCollectionName);
            // Configure to expire metrics for old clients if not updated for longer than a minute
            int defaultTimeToLive = 60;
            if (metricCollection == null)
            {
                Console.WriteLine("Creating metric collection {0}", MetricCollectionName);
                DocumentCollection metricCollectionDefinition = new DocumentCollection();
                metricCollectionDefinition.Id = MetricCollectionName;
                metricCollectionDefinition.DefaultTimeToLive = defaultTimeToLive;

                metricCollection = await ExecuteWithRetries<ResourceResponse<DocumentCollection>>(
                   client,
                   () => client.CreateDocumentCollectionAsync(
                       UriFactory.CreateDatabaseUri(DatabaseName),
                       new DocumentCollection { Id = MetricCollectionName },
                       new RequestOptions { OfferThroughput = 10000 }), 
                   true);
            }
            else
            {
                metricCollection.DefaultTimeToLive = defaultTimeToLive;
                await client.ReplaceDocumentCollectionAsync(metricCollection);
            }

            Console.WriteLine("Starting Inserts with {0} tasks", TaskCount);
            string sampleDocument = File.ReadAllText(ConfigurationManager.AppSettings["DocumentTemplateFile"]);
            var start = DateTime.Now;
            var tasks = new List<Task>();
            for (int i = 1; i <= 100; i++)
            {
                tasks.Add((RunBulkImport(collection.SelfLink)));
             }
            await Task.WhenAll(tasks);
            pendingTaskCount = TaskCount;
           // var tasks = new List<Task>();
           // tasks.Add(RunBulkImport(collection.SelfLink));
            /*tasks.Add(this.LogOutputStats());
            var start = DateTime.Now;
            long numberOfDocumentsToInsert = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToInsert"])/TaskCount;
            for (var i = 0; i < TaskCount; i++)
            {
                tasks.Add(RunBulkImport(collection.SelfLink));
            }

            await Task.WhenAll(tasks);*/
            Console.WriteLine("{0}", (DateTime.Now - start));
            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Console.WriteLine("Deleting Database {0}", DatabaseName);
                await ExecuteWithRetries<ResourceResponse<Database>>(
                   client,
                   () => client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName)),
                   true);
            }
            Console.ReadKey();
        }

        private async Task InsertDocument(int taskId, DocumentClient client, DocumentCollection collection, string sampleJson, long numberOfDocumentsToInsert)
        {
            requestUnitsConsumed[taskId] = 0;
            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");
            Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary[partitionKeyProperty] = Guid.NewGuid().ToString();

                try
                {
                    ResourceResponse<Document> response = await ExecuteWithRetries<ResourceResponse<Document>>(
                        client,
                        () => client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollectionName),
                            newDictionary,
                            new RequestOptions() { }));

                    string partition = response.SessionToken.Split(':')[0];
                    requestUnitsConsumed[taskId] += response.RequestCharge;
                    Interlocked.Increment(ref documentsInserted);
                }
                catch (Exception e)
                {
                    Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(newDictionary), e);
                }
            }

            Interlocked.Decrement(ref pendingTaskCount);
        }

        private async Task LogOutputStats()
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                long currentCount = documentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    Math.Round(documentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));                   


                Dictionary<string, object> latestStats = new Dictionary<string, object>();
                latestStats["id"] = string.Format("latest{0}", InstanceId);
                latestStats["type"] = "latest";
                latestStats["totalDocumentsCreated"] = currentCount;
                latestStats["documentsCreatedPerSecond"] = Math.Round(documentsInserted / seconds);
                latestStats["requestUnitsPerSecond"] = Math.Round(ruPerSecond);
                latestStats["requestUnitsPerMonth"] = Math.Round(ruPerSecond) * 86400 * 30;
                latestStats["documentsCreatedInLastSecond"] = Math.Round((currentCount - lastCount) / (seconds - lastSeconds));
                latestStats["requestUnitsInLastSecond"] = Math.Round((requestUnits - lastRequestUnits) / (seconds - lastSeconds));
                latestStats["requestUnitsPerMonthBasedOnLastSecond"] =
                    Math.Round(((requestUnits - lastRequestUnits) / (seconds - lastSeconds)) * 86400 * 30);

                await InsertMetricsToDocumentDB(latestStats);

                lastCount = documentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(documentsInserted / watch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            Console.WriteLine("--------------------------------------------------------------------- ");
        }

        private async Task InsertMetricsToDocumentDB(Dictionary<string, object> latestStats)
        {
            try
            {
                await ExecuteWithRetries<ResourceResponse<Document>>(
                    client,
                    () => client.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(DatabaseName, MetricCollectionName),
                        latestStats));
            }
            catch (Exception e)
            {
                Trace.TraceError("Insert metrics document failed with {0}", e);
            }
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        private async Task<DocumentCollection> CreatePartitionedCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add(ConfigurationManager.AppSettings["CollectionPartitionKey"]);

            // Show user cost of running this test
            double estimatedCostPerMonth = 0.06 * CollectionThroughput;
            double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);
            Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", estimatedCostPerHour, estimatedCostPerMonth);
            Console.WriteLine("Press enter to continue ...");
            Console.ReadLine();

            return await ExecuteWithRetries<ResourceResponse<DocumentCollection>>(
                client,
                () => client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName), 
                    collection, 
                    new RequestOptions { OfferThroughput = CollectionThroughput }));
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        private Database GetDatabaseIfExists(string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested collection</returns>
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Execute the function with retries on throttle.
        /// </summary>
        /// <typeparam name="V">The type of return value from the execution.</typeparam>
        /// <param name="client">The DocumentDB client instance.</param>
        /// <param name="function">The function to execute.</param>
        /// <returns>The response from the execution.</returns>
        public static async Task<V> ExecuteWithRetries<V>(DocumentClient client, Func<Task<V>> function, bool shouldLogRetries = false)
        {
            TimeSpan sleepTime = TimeSpan.Zero;
            int[] expectedStatusCodes = new int[] { 429, 400, 503 };

            while (true)
            {
                try
                {
                    return await function();
                }
                catch (System.Net.Http.HttpRequestException)
                {
                    sleepTime = TimeSpan.FromSeconds(1);
                }
                catch (Exception e)
                {
                    DocumentClientException de;
                    if (!TryExtractDocumentClientException(e, out de))
                    {
                        throw;
                    }

                    sleepTime = de.RetryAfter;
                    if (shouldLogRetries)
                    {
                        Console.WriteLine("Retrying after sleeping for {0}", sleepTime);
                    }
                }

                await Task.Delay(sleepTime);
            }
        }

        private static bool TryExtractDocumentClientException(Exception e, out DocumentClientException de)
        {
            if (e is DocumentClientException)
            {
                de = (DocumentClientException)e;
                return true;
            }

            if (e is AggregateException)
            {
                if (e.InnerException is DocumentClientException)
                {
                    de = (DocumentClientException)e.InnerException;
                    return true;
                }
            }

            de = null;
            return false;
        }
        private static async Task<DocumentCollection> CreateCollectionAsync(string dbLink, string id)
        {
            DocumentCollection collectionDefinition = new DocumentCollection { Id = id };
            collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
            collectionDefinition.PartitionKey.Paths.Add("/LastName");

            return client.CreateDocumentCollectionQuery(dbLink)
                .Where(c => c.Id == id).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a new database for this ID
        /// </summary>
        /// <param name="id">The id of the Database to search for, or create.</param>
        /// <returns>The matched, or created, Database object</returns>
        private static async Task<Database> GetNewDatabaseAsync(string id)
        {
            Database database = client.CreateDatabaseQuery().Where(db => db.Id == id).ToArray().FirstOrDefault();
            if (database != null)
            {
               // database = await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(id));
            }

            return database;
        }
        private static async Task RunBulkImport(string collectionLink)
        {
            string inputDirectory = @".\Data\";
            string inputFileMask = "*.json";
            int maxFiles = 2000;
            int maxScriptSize = 50000;

            // 1. Get the files.
            string[] fileNames = Directory.GetFiles(inputDirectory, inputFileMask);
            DirectoryInfo di = new DirectoryInfo(inputDirectory);
            FileInfo[] fileInfos = di.GetFiles(inputFileMask);

            // 2. Prepare for import.
            int currentCount = 0;
            int fileCount = maxFiles != 0 ? Math.Min(maxFiles, fileNames.Length) : fileNames.Length;

            // 3. Create stored procedure for this script.
            

            // 4. Create a batch of docs (MAX is limited by request size (2M) and to script for execution.           
            // We send batches of documents to create to script.
            // Each batch size is determined by MaxScriptSize.
            // MaxScriptSize should be so that:
            // -- it fits into one request (MAX reqest size is 16Kb).
            // -- it doesn't cause the script to time out.
            // -- it is possible to experiment with MaxScriptSize to get best perf given number of throttles, etc.
            while (currentCount < 10)
            {
                // 5. Create args for current batch.
                //    Note that we could send a string with serialized JSON and JSON.parse it on the script side,
                //    but that would cause script to run longer. Since script has timeout, unload the script as much
                //    as we can and do the parsing by client and framework. The script will get JavaScript objects.
                String s = "" + ((++p) % 100);
                string argsJson = CreateBulkInsertScriptArguments(fileNames, currentCount, fileCount, maxScriptSize);
                var args = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(argsJson) };
                
              
                StoredProcedureResponse<int> scriptResult = await client.ExecuteStoredProcedureAsync<int>(
                    sproc.SelfLink,
                    new RequestOptions { PartitionKey = new PartitionKey(s) },
                    args);

                // 7. Prepare for next batch.
                int currentlyInserted = scriptResult.Response;
                currentCount += currentlyInserted;
            }

            // 8. Validate
            int numDocs = 0;
            string continuation = string.Empty;
           
        }
        private static string CreateBulkInsertScriptArguments(string[] docFileNames, int currentIndex, int maxCount, int maxScriptSize)
        {
            ii++;
            var jsonDocumentArray = new StringBuilder();
            jsonDocumentArray.Append("[");
         
            if (currentIndex >= maxCount) return string.Empty;
         
            Dictionary<string, object> newDictionary =
            JsonConvert.DeserializeObject<Dictionary<string, object>>((File.ReadAllText(docFileNames[currentIndex])));
            var sn = ii.ToString("D14");
            newDictionary["id"] = Guid.NewGuid().ToString();
            newDictionary["LastName"] = "" + ((p) % 100);
            jsonDocumentArray.Append(JsonConvert.SerializeObject(newDictionary));
   

            int scriptCapacityRemaining = maxScriptSize;
            string separator = string.Empty;

            int i = 1;
            while (jsonDocumentArray.Length < scriptCapacityRemaining && (currentIndex + i) < 10)
            {
                ii++;
               
                sn = ii.ToString("D14");
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary["LastName"] = "" + ((p) % 100);
                jsonDocumentArray.Append(", " + JsonConvert.SerializeObject(newDictionary));
                i++;
               // Console.WriteLine(newDictionary["LastName"]);
            }

            jsonDocumentArray.Append("]");
            
            return jsonDocumentArray.ToString();
        }
        private static async Task TryDeleteStoredProcedure(string collectionLink, string sprocId)
        {
            StoredProcedure sproc = client.CreateStoredProcedureQuery(collectionLink).Where(s => s.Id == sprocId).AsEnumerable().FirstOrDefault();
            if (sproc != null)
            {
                await client.DeleteStoredProcedureAsync(sproc.SelfLink);
            }
        }

    }
}
