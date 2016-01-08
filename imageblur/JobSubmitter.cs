using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.FileStaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Collections.Concurrent;

namespace imageblur
{
    class JobSubmitter
    {

        // required files on the compute nodes that run the tasks
        private const string ImageBlurExeName = "ImageBlur.exe";
        private const string StorageClientDllName = "Microsoft.WindowsAzure.Storage.dll";
        private const string ImageProcessorDllName = "ImageProcessor.dll";
        public static void JobMain(string[] args)
        {
            Console.WriteLine("Setting up Batch Process - ImageBlur. \nPress Enter to begin.");
            Console.WriteLine("-------------------------------------------------------------");
            Console.ReadLine();
            Settings imageBlurSettings = Settings.Default;
            AccountSettings accountSettings = AccountSettings.Default;

            /* Setting up credentials for Batch and Storage accounts
             * =====================================================
             */

            StorageCredentials storageCredentials = new StorageCredentials(
                accountSettings.StorageAccountName, 
                accountSettings.StorageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, useHttps: true);

            StagingStorageAccount stagingStorageAccount = new StagingStorageAccount(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                storageAccount.BlobEndpoint.ToString());

            BatchSharedKeyCredentials batchCredentials = new BatchSharedKeyCredentials(
                accountSettings.BatchServiceUrl, 
                accountSettings.BatchAccountName, 
                accountSettings.BatchAccountKey);


            using (BatchClient client = BatchClient.Open(batchCredentials))
            {
                string stagingContainer = null;

                /* Setting up pool to run job and tasks in
                 * =======================================
                 */

                CreatePool(client, imageBlurSettings, accountSettings);

                try
                {

                    /* Setting up Job ------------------------
                     * =======================================
                     */

                    Console.WriteLine("Creating job {0}. \nPress Enter to continue.", imageBlurSettings.JobId);
                    Console.ReadLine();

                    CloudJob unboundJob = client.JobOperations.CreateJob();
                    unboundJob.Id = imageBlurSettings.JobId;
                    unboundJob.PoolInformation = new PoolInformation() { PoolId = imageBlurSettings.PoolId };
                    unboundJob.Commit();


                    /* Uploading Source Image(s) to run varying degrees of Blur on
                     * ===========================================================
                     * Here, the input data is uploaded separately to Storage and 
                     * its URI is passed to the task as an argument.
                     */

                    Console.WriteLine("Uploading source images. \nPress Enter to continue.");
                    Console.ReadLine();

                    string[] sourceImages = imageBlurSettings.SourceImageNames.Split(',');
                    List<String> sourceImageUris = new List<String>();
                    for( var i = 0; i < sourceImages.Length; i++)
                    {
                        Console.WriteLine("    Uploading {0}.", sourceImages[i]);
                        sourceImageUris.Add( UploadSourceImagesFileToCloudBlob(accountSettings, sourceImages[i]));
                        Console.WriteLine("    Source Image uploaded to: <{0}>.", sourceImageUris[i]);
                    }

                    Console.WriteLine();
                    Console.WriteLine("All Source Images uploaded. \nPress Enter to continue.");
                    Console.ReadLine();

                    /* Setting up tasks with dependencies ----------------
                     * ===================================================
                     */

                    Console.WriteLine("Setting up files to stage for tasks. \nPress Enter to continue.");
                    Console.ReadLine();

                    // Setting up Files to Stage - Files to upload into each task (executables and dependent assemblies)
                    FileToStage imageBlurExe = new FileToStage(ImageBlurExeName, stagingStorageAccount);
                    FileToStage storageDll = new FileToStage(StorageClientDllName, stagingStorageAccount);
                    FileToStage imageProcessorDll = new FileToStage(ImageProcessorDllName, stagingStorageAccount);

                    // initialize collection to hold tasks that will be submitted in their entirety
                    List<CloudTask> tasksToRun = new List<CloudTask>(imageBlurSettings.NumberOfTasks);

                    for (int i = 0; i < imageBlurSettings.NumberOfTasks; i++)
                    {
                        // create individual tasks (cmd line passed in as argument)
                        CloudTask task = new CloudTask("task_" + i, String.Format("{0} --Task {1} {2} {3}",
                            ImageBlurExeName,
                            sourceImageUris[i],
                            accountSettings.StorageAccountName,
                            accountSettings.StorageAccountKey));

                        // list of files to stage to a container -- for each job, one container is created and
                        // files all resolve to Azure Blobs by their name
                        task.FilesToStage = new List<IFileStagingProvider> { imageBlurExe, storageDll, imageProcessorDll };

                        tasksToRun.Add(task);
                        Console.WriteLine("\t task {0} has been added", "task_" + i);
                    }
                    Console.WriteLine();

                    /* Commit tasks with dependencies ----------------
                     * ===============================================
                     */

                    Console.WriteLine("Running Tasks. \nPress Enter to continue.");
                    Console.WriteLine("-------------------------------------------------------------");
                    Console.ReadLine();

                    ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>> fsArtifactBag = new ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>>();
                    client.JobOperations.AddTask(imageBlurSettings.JobId, tasksToRun, fileStagingArtifacts: fsArtifactBag);

                    foreach (var fsBagItem in fsArtifactBag)
                    {
                        IFileStagingArtifact fsValue;
                        if (fsBagItem.TryGetValue(typeof(FileToStage), out fsValue))
                        {
                            SequentialFileStagingArtifact stagingArtifact = fsValue as SequentialFileStagingArtifact;
                            if (stagingArtifact != null)
                            {
                                stagingContainer = stagingArtifact.BlobContainerCreated;
                                Console.WriteLine(
                                    "Uploaded files to container: {0} -- \nyou will be charged for their storage unless you delete them.",
                                    stagingArtifact.BlobContainerCreated);
                            }
                        }
                    }

                    //Get the job to monitor status.
                    CloudJob job = client.JobOperations.GetJob(imageBlurSettings.JobId);

                    Console.WriteLine();
                    Console.Write("Waiting for tasks to complete ...   ");
                    IPagedEnumerable<CloudTask> ourTasks = job.ListTasks(new ODATADetailLevel(selectClause: "id"));
                    client.Utilities.CreateTaskStateMonitor().WaitAll(ourTasks, TaskState.Completed, TimeSpan.FromMinutes(20));
                    Console.WriteLine("tasks are done.");
                    Console.WriteLine();

                    Console.WriteLine("See below for Stdout / Stderr for each node.");
                    Console.WriteLine("============================================");

                    /* Display stdout/stderr for each task on completion 
                     * =================================================
                     */

                    foreach (CloudTask t in ourTasks)
                    {
                        Console.WriteLine("Task " + t.Id + ":");
                        Console.WriteLine("    stdout:" + Environment.NewLine + t.GetNodeFile("stdout.txt").ReadAsString());
                        Console.WriteLine();
                        Console.WriteLine("    stderr:" + Environment.NewLine + t.GetNodeFile("stderr.txt").ReadAsString());
                    }

                    Console.WriteLine();
                    Console.WriteLine("Please find the resulting images in storage. \nPress Enter to continue.");
                    Console.WriteLine("=======================================================================");
                    Console.ReadLine();
                }
                finally
                {
                    /* If configured as such, Delete the resources that were used in this process
                     * ==========================================================================
                     */

                    //Delete the pool that we created
                    if (imageBlurSettings.DeletePool)
                    {

                        Console.WriteLine("Deleting Pool. \nPress Enter to continue.");
                        Console.ReadLine();

                        Console.WriteLine("Deleting pool: {0}", imageBlurSettings.PoolId);
                        client.PoolOperations.DeletePool(imageBlurSettings.PoolId);
                    }

                    //Delete the job that we created
                    if (imageBlurSettings.DeleteJob)
                    {

                        Console.WriteLine("Deleting Job. \nPress Enter to continue.");
                        Console.ReadLine();

                        Console.WriteLine("Deleting job: {0}", imageBlurSettings.JobId);
                        client.JobOperations.DeleteJob(imageBlurSettings.JobId);
                    }

                    //Delete the containers we created
                    if (imageBlurSettings.DeleteContainer)
                    {

                        Console.WriteLine("Deleting Container. \nPress Enter to continue.");
                        Console.ReadLine();

                        DeleteContainers(accountSettings, stagingContainer);
                    }
                    Console.WriteLine();
                    Console.WriteLine("Please check the Azure portal to make sure that all resources you want deleted are in fact deleted");
                    Console.WriteLine("==================================================================================================");
                    Console.WriteLine();
                    Console.WriteLine("Press Enter to exit the program");
                    Console.WriteLine("Exiting program...");
                }

            }

        }

        private static string sourceImageContainerName = "sourceimagecontainer";

        private static void DeleteContainers(AccountSettings accountSettings, string fileStagingContainer)
        {
            CloudBlobClient client = GetCloudBlobClient(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                accountSettings.StorageServiceUrl);

            //Delete the books container
            CloudBlobContainer container = client.GetContainerReference(sourceImageContainerName);
            Console.WriteLine("Deleting container: " + sourceImageContainerName);
            container.DeleteIfExists();

            //Delete the file staging container
            if (!string.IsNullOrEmpty(fileStagingContainer))
            {
                container = client.GetContainerReference(fileStagingContainer);
                Console.WriteLine("Deleting container: {0}", fileStagingContainer);
                container.DeleteIfExists();
            }

        }

        private static void CreatePool(BatchClient client, Settings imageBlurSettings, AccountSettings accountSettings)
        {
            CloudPool pool = client.PoolOperations.CreatePool(
                imageBlurSettings.PoolId,
                targetDedicated: imageBlurSettings.PoolNodeCount,
                osFamily: "4",
                virtualMachineSize: "large");

            try
            {
                Console.WriteLine("Adding pool {0}", imageBlurSettings.PoolId);
                pool.Commit();
                Console.WriteLine("pool {0} has been created. \nPress Enter to Continue.", imageBlurSettings.PoolId);
                Console.ReadLine();
            }
            catch (AggregateException ae)
            {
                // Go through all exceptions and dump useful information
                ae.Handle(x =>
                {
                    Console.Error.WriteLine("Creating pool ID {0} failed", imageBlurSettings.PoolId);
                    if (x is BatchException)
                    {
                        BatchException be = x as BatchException;

                        Console.WriteLine(be.ToString());
                        Console.WriteLine();
                    }
                    else
                    {
                        Console.WriteLine(x);
                    }

                    // can't continue without a pool
                    return false;
                });
            }
        }

        private static CloudBlobClient GetCloudBlobClient(string accountName, string accountKey, string accountUrl)
        {
            StorageCredentials cred = new StorageCredentials(accountName, accountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, accountUrl, useHttps: true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            return client;
        }

        private static string UploadSourceImagesFileToCloudBlob(AccountSettings accountSettings, string fileName)
        {
            CloudBlobClient client = GetCloudBlobClient(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                accountSettings.StorageServiceUrl);

            //Create the "sourceimage" container if it doesn't exist.
            CloudBlobContainer container = client.GetContainerReference(sourceImageContainerName);
            container.CreateIfNotExists();

            // set permissions on the container
            //BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            //containerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;
            //container.SetPermissions(containerPermissions);

            //Upload the blob.
            CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
            blob.UploadFromFile(fileName, FileMode.Open);
            return blob.Uri.ToString();
        }


    }
}
