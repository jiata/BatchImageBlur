using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ImageProcessor;
using System.IO;
using ImageProcessor.Imaging.Formats;
using System.Drawing;
using System.Drawing.Imaging;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;

namespace imageblur
{
    class ImageBlur
    {
        public static void TaskMain(string[] args)
        {
            if (args == null || args.Length != 4)
            {
                throw new Exception("Usage: ImageBlur.exe --Task <blobpath> <storageAccountName> <storageAccountKey>");
            }

            string blobName = args[1];
            string storageAccountName = args[2];
            string storageAccountKey = args[3];
            string workingDirectory = Environment.GetEnvironmentVariable("AZ_BATCH_TASK_WORKING_DIR");
            int numberToBlur = 3;

            Console.WriteLine();
            Console.WriteLine("    blobName: <{0}>", blobName);
            Console.WriteLine("    storageAccountName: <{0}>", storageAccountName);
            Console.WriteLine("    number to blur: <{0}>", numberToBlur);
            Console.WriteLine();

            // get source image from cloud blob
            var storageCred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobName), storageCred);

            using (MemoryStream inStream = new MemoryStream())
            {
                blob.DownloadToStream(inStream);
                Image img = Image.FromStream(inStream);
                int imgWidth = img.Width;
                int imgHeight = img.Height;
                Size size = new Size(imgWidth, imgHeight);
                ISupportedImageFormat format = new JpegFormat { Quality = 70 };

                // Print image properties to stdout
                Console.WriteLine("    Image Properties:");
                Console.WriteLine("        Format: {0}", FormatUtilities.GetFormat(inStream));
                Console.WriteLine("        Size: {0}", size.ToString());
                Console.WriteLine();

                for (var i = 0; i < numberToBlur; i++)
                {
                    using (ImageFactory imageFactory = new ImageFactory(preserveExifData: true))
                    {
                        int blurIndex = (i * 5) + 10;
                        imageFactory.Load(inStream);
                        imageFactory.Resize(size)
                                    .Format(format)
                                    .GaussianBlur(blurIndex)
                                    .Save(workingDirectory + "/resultimage" + i + ".Jpeg");
                                    //.Save(@"C:/Users/jiata/Desktop/imageblur/results/resultimage" + i + ".Jpeg");
                    }
                }
            }

            // TODO - not working
            for (var i = 0; i < numberToBlur; i++)
            {
                blob.UploadFromFile(workingDirectory + "/resultimage" + i + ".Jpeg", FileMode.Open);
            }

            Environment.Exit(1);
        }
    }
}
