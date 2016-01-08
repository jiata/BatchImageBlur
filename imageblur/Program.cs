using ImageProcessor;
using System.IO;
using ImageProcessor.Imaging.Formats;
using System.Drawing;
using System.Drawing.Imaging;
using System;

namespace imageblur
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args != null && args.Length > 0 && args[0] == "--Task")
            {
                ImageBlur.TaskMain(args);
            } 
            else
            {
                JobSubmitter.JobMain(args);
            }
        }
    }
}
