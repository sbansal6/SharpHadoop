using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharpHadoop
{
  public class Example
    {
      public void Main()
      {
           WebHDFS hdfs = new WebHDFS("namenodeURL", "username", "50070");
          
          // List files in Directory
          string jsonString = hdfs.ListDir("pathToDir");
          Console.WriteLine(jsonString);

          // Create Directory
          hdfs.MkDir("path/folderName");

          // Remove Directory/file
          hdfs.RmDir("path/folderName");
          hdfs.RmDir("path/fileName");

          // Copy file from local to hdfs (Upload)
          hdfs.CopyFromLocal(@"localFullPath", "hdfsDesinationFolder", 1);

          // Copy from hdfs to local (Download)
          hdfs.copyToLocal("hdfsSourcefolder", @"localFullPath");

          // You can use json.net to parse results of listdir to object or datatable.



      }
    }
}
