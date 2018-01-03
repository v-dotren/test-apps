// WinFFmpeg1114 is a .NET Framework console app project using Batch SDK for .NET.
// Demonstrates a basic Batch pool that processes ffmpeg as the task.

namespace WinFFmpeg1114
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Batch;
    using Microsoft.Azure.Batch.Auth;
    using Microsoft.Azure.Batch.Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    class Program
    {
        // Update the Batch and Storage account credential strings below with the values unique to your accounts.
        // These are used when constructing connection strings for the Batch and Storage client objects.

        // Batch account credentials
        private const string BatchAccountName = "batchacct1212";
        private const string BatchAccountKey = "QqvbF4t8AZfQQNDUfI285P+YtCFZGP6ljjEVmTyMZYzaZt45zYsJFZgUbU3mnjiMAPCWF/m8MaGbCPmzrtjNXA==";
        private const string BatchAccountUrl = "https://batchacct1212.eastus.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "generalstorage1212";
        private const string StorageAccountKey = "KdtMjcJBjX7Wh14PTV26WMHaX29KEsuOhygyEZFjDqw1kCIQeYCpghHXV/ZZa3zlFAAqTCMbcZ2LLZzktFVdRA==";

        // Pool and Job IDs
        private const string PoolId = "WinFFmpegPool1220c";
        private const string JobId = "WinFFmpegJob1220c";

        public static void Main(string[] args)
        {
            if (String.IsNullOrEmpty(BatchAccountName) || String.IsNullOrEmpty(BatchAccountKey) || String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) || String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            try
            {
                // START TIMER
                Console.WriteLine("Sample start: {0}", DateTime.Now);
                Console.WriteLine();
                Stopwatch timer = new Stopwatch();
                timer.Start();


                // STORAGE SETUP

                // Construct the Storage account connection string
                string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                                StorageAccountName, StorageAccountKey);

                // Retrieve the storage account
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

                // Create the blob client, which is used to obtain references to blob storage containers
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                // Use the blob client to create the containers in blob storage
                const string appContainerName = "application-1220c";
                const string inputContainerName = "input-1220c";
                const string outputContainerName = "output-1220c";
                CreateContainerIfNoneExist(blobClient, inputContainerName);
                CreateContainerIfNoneExist(blobClient, appContainerName);
                CreateContainerIfNoneExist(blobClient, outputContainerName);


                // RESOURCE FILE SETUP

                // Application file: Define the paths to the executable that the tasks will use.
                // The application file is an executable (ffmpeg.exe) that converts the MPG input
                // files to AVI format output files.
                List<string> applicationFilePaths = new List<string>
                {
                    // The application file (ffmpeg.exe) resides in \<solutiondir>\<projectdir>\AppFiles.
                    @"..\..\AppFiles\ffmpeg.exe"
                };

                // Input files: Specify the location of the data files that the tasks process, and
                // put them in a List collection. Make sure you have copied the data files to:
                // \<solutiondir>\<projectdir>\InputFiles.

                List<string> inputFilePaths = new List<string>
                {
                    @"..\..\InputFiles\LowPriVMs-1.mp4",
                    @"..\..\InputFiles\LowPriVMs-2.mp4",
                    @"..\..\InputFiles\LowPriVMs-3.mp4",
                    @"..\..\InputFiles\LowPriVMs-4.mp4",
                    @"..\..\InputFiles\LowPriVMs-5.mp4"
                };

                // Upload the application executable file to the blob container, using
                // UploadResourceFilesToContainer() defined below. Each of the tasks on the
                // compute nodes execute this application to process the data files.
                List<ResourceFile> applicationFiles = UploadResourceFilesToContainer(blobClient, appContainerName, applicationFilePaths);

                // Upload the data files to the input container. This data will be processed by
                // each of the tasks that are executed on the compute nodes in the pool.
                List<ResourceFile> inputFiles = UploadResourceFilesToContainer(blobClient, inputContainerName, inputFilePaths);

                // Obtain a shared access signature that provides write access to the output container
                // to which the tasks upload their output.
                string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);


                // CREATE BATCH CLIENT / CREATE POOL / CREATE JOB / ADD TASKS

                // Create a Batch client and authenticate with shared key credentials.
                // The Batch client allows the app to interact with the Batch service.
                BatchSharedKeyCredentials sharedKeyCredentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
                using (BatchClient batchClient = BatchClient.Open(sharedKeyCredentials))
                {
                    // Create the Batch pool, which contains the compute nodes that execute the tasks.
                    // This method defines a start task that is executed each time a compute node joins
                    // the pool, or is rebooted or reimaged. Pass in a collection of one or more application
                    // files to be used to run the pool's start task.
                    CreatePoolIfNoneExist(batchClient, PoolId, applicationFiles);

                    // Create the job that runs the tasks.
                    CreateJob(batchClient, JobId, PoolId);

                    // Create a collection of tasks and add them to the Batch job. Define a Batch node
                    // shared directory, where the task application executable has been copied. The task
                    // can access the application via the shared directory on whichever node each task runs.
                    // Provide a shared access signature for the tasks so that they can upload their output
                    // to the Storage container.
                    AddTasks(batchClient, JobId, inputFiles, outputContainerSasUrl);

                    // Monitor task success or failure, specifying a maximum amount of time to wait for
                    // the tasks to complete.
                    MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(30)).Wait();

                    // Print out timing info
                    timer.Stop();
                    Console.WriteLine();
                    Console.WriteLine("Sample end: {0}", DateTime.Now);
                    Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                    // Clean up Batch resources

                    // %% NEW
                    // Prompt user to delete Batch job
                    Console.WriteLine();
                    Console.Write("Delete job [{0}] ? yes [no]: ", JobId);
                    string response = Console.ReadLine().ToLower();
                    if (response != "y" && response != "yes")
                    {
                        Console.Write("Reminder: Job will remain in Azure portal until you delete it.");
                    }
                    else
                    {
                        batchClient.JobOperations.DeleteJob(JobId);
                    }

                    // Prompt user to delete Batch pool
                    Console.WriteLine();
                    Console.Write("Delete pool [{0}] ? [yes] no: ", PoolId);
                    response = Console.ReadLine().ToLower();
                    if (response != "y" && response != "yes")
                    {
                        Console.Write("Reminder: Pool will remain in Azure portal until you delete it.");
                    }
                    else
                    {
                        batchClient.PoolOperations.DeletePool(PoolId);
                    }

                    // Remind user to delete application Storage container
                    Console.WriteLine();
                    Console.Write("Reminder: After you view files in container [{0}], make sure to delete it in Azure Portal.", appContainerName);

                    // Remind user to delete input Storage container
                    Console.WriteLine();
                    Console.Write("Reminder: After you view files in container [{0}], make sure to delete it in Azure Portal.", inputContainerName);

                    // Remind user to delete output Storage container
                    Console.WriteLine();
                    Console.Write("Reminder: After you view files in container [{0}], make sure to delete it in Azure Portal.", outputContainerName);
                }
            }
            catch (AggregateException ae)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();

                PrintAggregateException(ae);
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete; press ENTER to exit.");
                Console.ReadLine();
            }
        }


        // METHOD IMPLEMENTATIONS

        // RESOURCE FILE SETUP - METHOD IMPLEMENTATIONS

        // CreateContainerIfNoneExist(): Creates a container with the specified name in Blob storage,
        // unless a container with that name already exists.
        // * blobClient: Reference to a cloud blob client (Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient).
        // * containerName: Name of the new blob storage container.
        private static void CreateContainerIfNoneExist(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (container.CreateIfNotExists())
            {
                Console.WriteLine("Container [{0}] created.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] exists, skipping creation.", containerName);
            }
        }

        // UploadResourceFilesToContainer(): Uploads the specified resource files to a container.
        //   * blobClient: Reference to a cloud blob client (Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient).
        //   * inputContainerName: Name of the blob storage container to which the files are uploaded.
        //   * filePaths: A collection of paths of the files to be uploaded to the container.
        //   Returns: A collection of <see cref="ResourceFile"/> objects.
        private static List<ResourceFile> UploadResourceFilesToContainer(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(UploadResourceFileToContainer(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        // UploadResourceFileToContainer(): Uploads the specified file to the specified blob container.
        //   Note that UploadResourceFilesToContainer() calls this method to upload individual files.
        //   * filePath: The full path to the file to upload to Storage.
        //   * blobClient: A cloud blob client object (Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient).
        //   * containerName: The name of the blob storage container to which the file should be uploaded.
        //   Returns: A ResourceFile object representing the file in blob storage.
        private static ResourceFile UploadResourceFileToContainer(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            // BlobRequestOptions and StreamWriteSizeInBytes are optional, but might be needed for
            // connections with slow upload times. This configuration specifies that files larger
            // than 1 MB be uploaded in 256 K size blocks.
            BlobRequestOptions blobRequestOptions = new BlobRequestOptions()
            {
                SingleBlobUploadThresholdInBytes = 1024 * 1024,  // files larger than 1 MB are uploaded in blocks
                ParallelOperationThreadCount = 1
            };

            blobClient.DefaultRequestOptions = blobRequestOptions;

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            blobData.StreamWriteSizeInBytes = 256 * 1024;        // Breaks the blob into 256 K blocks for upload
            blobData.UploadFromFile(filePath);

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }

        // GetContainerSasUrl(): Returns a shared access signature (SAS) URL providing the specified
        //   permissions to the specified container. The SAS URL provided is valid for 2 hours from
        //   the time this method is called. The container must already exist in Azure Storage.
        //   * blobClient: A CloudBlobClient object (Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient).
        //   * containerName: The name of the container for which a SAS URL will be obtained. The 
        //     specified container must already exist in Azure Storage.
        //   * permissions: The permissions granted by the SAS URL.
        //   Returns: An SAS URL providing the specified access to the container. Note this URL is
        //     valid for 2 hours from the time this method is called.
        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
        {
            // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };

            // Generate the shared access signature on the container, setting the constraints directly on the signature
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URL string for the container, including the SAS token
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }


        // BATCH CLIENT SETUP - METHOD IMPLEMENTATIONS

        // CreatePoolIfNoneExist(): Creates the Batch pool.
        //   batchClient: A BatchClient object.
        //   PoolId: ID of the CloudPool object to create.
        //   resourceFiles: A collection of ResourceFile objects representing blobs in a Storage account container.
        private static void CreatePoolIfNoneExist(BatchClient batchClient, string poolId, IList<ResourceFile> resourceFiles)
        {
            CloudPool pool = null;
            try
            {
                Console.WriteLine("Creating pool [{0}]...", poolId);

                // Create an unbound pool. No pool is actually created in the Batch service until we call
                // CloudPool.Commit(). This CloudPool instance is therefore considered "unbound," and we can
                // modify its properties.
                pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetDedicatedComputeNodes: 5,                                             // 5 compute nodes
                    virtualMachineSize: "small",                                                // single-core, 1.75 GB memory, 225 GB disk
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));   // Windows Server 2012 R2

                pool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", poolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        // CreateJob(): Creates a job in the specified pool.
        //   batchClient: A BatchClient object.
        //   jobId: ID of the job to create.
        //   poolId: ID of the CloudPool object in which to create the job.
        private static void CreateJob(BatchClient batchClient, string jobId, string poolId)
        {
            Console.WriteLine("Creating job [{0}]...", jobId);

            CloudJob job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            job.Commit();
        }

        // AddTasks(): Creates tasks to process each of the specified input files, and submits them
        //   to the specified job for execution.
        //     batchClient: A BatchClient object.
        //     jobId: The ID of the job to which the tasks are added.
        //     inputFiles: A collection of ResourceFile objects representing the input files
        //       to be processed by the tasks executed on the compute nodes.
        //     outputContainerSasUrl: The shared access signature URL for the Azure Storage
        //       container that will hold the output files that the tasks create.
        //   Returns: A collection of the submitted cloud tasks.
        private static List<CloudTask> AddTasks(BatchClient batchClient, string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
        {
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, jobId);

            // Create a collection to hold the tasks added to the job:
            List<CloudTask> tasks = new List<CloudTask>();

            // Create each task. The start task copies the application executable (ffmpeg.exe) to the
            // node's shared directory, so the cloud tasks can access this application via the shared
            // directory on whichever node each task runs.

            foreach (ResourceFile inputFile in inputFiles)
            {
                // Assign a task ID for each iteration
                string taskId = "task_" + inputFiles.IndexOf(inputFile);

                // Define task command line to convert the video format from MP4 to MPEG-1 using ffmpeg.
                // Note that ffmpeg syntax specifies the format conversion using the file extension of
                // the input file and the output file respectively.

                /*
                // %% REPLACE THIS BLOCK WITH DAN'S OUTPUT FILE FORMATTING
                string inputFilePath = inputFile.FilePath;
                string outputFileName = String.Format("{0}{1}",
                    System.IO.Path.GetFileNameWithoutExtension(inputFilePath),
                    ".mpeg");
                string outputFilePath = outputContainerSasUrl + outputFileName;
                */

                // %% NEW OUTPUT FILE FORMATTING:
                // Define paths for input and output files
                string inputMediaFile = inputFile.FilePath;
                string outputMediaFile = String.Format("{0}{1}",
                    System.IO.Path.GetFileNameWithoutExtension(inputMediaFile),
                    ".mpeg");
                // Format the task command line
                string taskCommandLine = String.Format("cmd /c %AZ_BATCH_NODE_SHARED_DIR%\\ffmpeg.exe -i {0} {1}", inputMediaFile, outputMediaFile);

                // Create a cloud task (with the task ID and command line) and give it a list of input files
                CloudTask task = new CloudTask(taskId, taskCommandLine);
                task.ResourceFiles = new List<ResourceFile> { inputFile };

                // Send the result to the task's list of output files
                List<OutputFile> outputFileList = new List<OutputFile>();
                OutputFileBlobContainerDestination outputContainer = new OutputFileBlobContainerDestination(outputContainerSasUrl);
                OutputFile outputFile = new OutputFile(outputMediaFile,
                                                       new OutputFileDestination(outputContainer),
                                                       new OutputFileUploadOptions(OutputFileUploadCondition.TaskSuccess));
                outputFileList.Add(outputFile);
                task.OutputFiles = outputFileList;

                // Add the cloud task to the task list
                tasks.Add(task);
            }

            // Call BatchClient.JobOperations.AddTask() to add the tasks as a collection rather than making a
            // separate call for each. Bulk task submission helps to ensure efficient underlying API
            // calls to the Batch service. calls AddTaskAsync() so the add operation doesn't hang up program execution.
            batchClient.JobOperations.AddTaskAsync(jobId, tasks).Wait();

            return tasks;
        }

        // MonitorTasks(): Asynchronously monitors the specified tasks for completion and returns a value indicating
        //   whether all tasks completed successfully within the timeout period.
        //   * batchClient: A BatchClient object.
        //   * jobId: The ID of the job containing the tasks to be monitored.
        //   * timeout: The period of time to wait for the tasks to reach the completed state.
        //   Returns: A Boolean indicating true if all tasks in the specified job completed successfully
        //      (with an exit code of 0) within the specified timeout period; otherwise false.
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string successMessage = "All tasks reached state Completed.";
            const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

            // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
            // specify that only the "id" property of each task should be populated. Using a detail level for
            // all list operations helps to lower response time from the Batch service.
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(JobId, detail).ToListAsync();

            Console.WriteLine("Awaiting task completion, timeout in {0}...", timeout.ToString());

            // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
            // reach the Completed state.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                Console.WriteLine(failureMessage);
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
            // or return a non-zero exit code.

            // Update the detail level to populate only the task id and executionInfo properties.
            // We refresh the tasks below, and need only this information for each task.
            detail.SelectClause = "id, executionInfo";

            foreach (CloudTask task in tasks)
            {
                // Populate the task's properties with the latest info from the Batch service
                await task.RefreshAsync(detail);

                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    // A task with failure information set indicates there was a problem with the task. It is important to note that
                    // the task's state can be "Completed," yet still have encountered a failure.

                    allTasksSuccessful = false;

                    Console.WriteLine("WARNING: Task [{0}] encountered a failure: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    if (task.ExecutionInformation.ExitCode != 0)
                    {
                        // A non-zero exit code may indicate that the application executed by the task encountered an error
                        // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                        // your implementation of error checking may differ from this example.

                        Console.WriteLine("WARNING: Task [{0}] returned a non-zero exit code - this may indicate task execution or completion failure.", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("Success! All tasks completed successfully within the specified timeout period.");
            }

            return allTasksSuccessful;
        }

        // DownloadBlobsFromContainer(): Downloads all files from the specified blob storage container
        //   to the specified directory.
        //   * blobClient: A CloudBlobClient object.
        //   * containerName: The name of the blob storage container containing the files to download.
        //   * directoryPath: The full path of the local directory to which the files should be downloaded.
        private static void DownloadBlobsFromContainer(CloudBlobClient blobClient, string containerName, string directoryPath)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", containerName);

            // Retrieve a reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Get a flat listing of all the block blobs in the specified container
            foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            {
                // Retrieve reference to the current blob
                CloudBlob blob = (CloudBlob)item;

                // Save blob contents to a file in the specified folder
                string localOutputFile = Path.Combine(directoryPath, blob.Name);
                blob.DownloadToFile(localOutputFile, FileMode.Create);
            }

            Console.WriteLine("All files downloaded to {0}", directoryPath);
        }

        // DeleteContainer(): Deletes the container with the specified name from Blob storage, unless
        //   a container with that name does not exist.
        //   * blobClient: A CloudBlobClient object.
        //   * containerName: The name of the container to delete.
        private static void DeleteContainer(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (container.DeleteIfExists())
            {
                Console.WriteLine("Container [{0}] deleted.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] does not exist, skipping deletion.", containerName);
            }
        }

        // PrintAggregateException(): Processes all exceptions inside an aggregate exception object
        //   and writes each inner exception to the console.
        //   * aggregateException: The AggregateException object to process.</param>
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Flatten the aggregate and iterate over its inner exceptions, printing each
            foreach (Exception exception in aggregateException.Flatten().InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }
    }
}
