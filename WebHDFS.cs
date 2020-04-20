using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using SharpHadoop.Utilities.Net;

namespace SharpHadoop
{
    ///<summary>
    ///C# Port of Python WebHDFS @ https://github.com/drelu/webhdfs-py
    ///</summary>
    public class WebHDFS
    {
         string namenodeHost {get;set;}
  string namenodePort {get;set;}
  string hdfsUsername {get;set;} 
  string WEBHDFS_CONTEXT_ROOT = "/webhdfs/v1" ; 
 
 ///<summary>
 ///Public Constructor takes two required, one optional parameters
 ///<param name="namenodeHost">Namenode Location without http://</param>
 ///<param name="hdfsUsername">UserName</param>
 ///<param name="namenodePort">Namenode Port, by default its 50070</param>
  ///</summary>
  public WebHDFS(string namenodeHost,string  hdfsUsername,string namenodePort = "50070")
  {
   this.namenodeHost = namenodeHost;
   this.namenodePort = namenodePort ;
   this.hdfsUsername = hdfsUsername ;
  }
  
  ///<summary>
 ///List items in directory
 ///<param name="path">Directory path to list.</param>
 ///<returns>Json string </returns>
 ///</summary>
  public string ListDir(string path)
  {
    // Create the final url with params
    string url_path = "http://" +this.namenodeHost + ":" +   this.namenodePort + WEBHDFS_CONTEXT_ROOT + "/"  + path +"?op=LISTSTATUS&user.name="+ this.hdfsUsername ;
    // create request using the above url
    HttpWebRequest req = WebRequest.Create(url_path) as HttpWebRequest;
    req.Method = WebRequestMethods.Http.Get; // Get method
    req.Accept = "application/json"; // accept json
    
    HttpWebResponse resp = req.GetResponse() as HttpWebResponse;
    StreamReader reader = new StreamReader(resp.GetResponseStream());
    
	string result = reader.ReadToEnd();
    return result ;
    
  }
  
  ///<summary>
  ///Creates directory in hdfs
  ///<param name="path">Directory path to create</param>
  ///<returns>HttpStatusCode</returns>
  ///</summary>
  public HttpStatusCode MkDir(string path)
  {
    // Create the final url with params
    string url_path = "http://" +this.namenodeHost + ":" +   this.namenodePort + WEBHDFS_CONTEXT_ROOT + "/"  + path +"?op=MKDIRS&user.name="+ this.hdfsUsername ;
    BetterWebClient wc = new BetterWebClient();
	wc.UploadString(url_path,"PUT","");
	return wc.StatusCode() ;		
  }
  
  ///<summary>
  ///Removes Directory or file in HDFS
  ///<param name="path">Directory/File path to delete</param>
  ///<returns>HttpStatusCode</returns>
  ///</summary>
  public HttpStatusCode RmDir(string path)
  {
    // Create the final url with params
    string url_path = "http://" +this.namenodeHost + ":" +   this.namenodePort + WEBHDFS_CONTEXT_ROOT + "/"  + path +"?op=DELETE&user.name="+ this.hdfsUsername ;
    BetterWebClient wc = new BetterWebClient();
	wc.UploadString(url_path,"DELETE","");
	return wc.StatusCode() ;		

  }

  ///<summary>
  ///Copy file from local to HDFS 
  ///<param name="sourcePath">Location of file on localhost, including filename and extension (FullFileName)</param>
  ///<param name="targetPath">Location of file on HDFS, including filename and extension (FullFileName)</param>
  ///<returns>HttpStatusCode</returns>
  ///</summary>
  ///<summary>
  ///Copy file from local to HDFS 
  ///<param name="sourcePath">Location of file on localhost, including filename and extension (FullFileName)</param>
  ///<param name="targetPath">Location of file on HDFS, including filename and extension (FullFileName)</param>
  ///<returns>HttpStatusCode</returns>
  ///</summary>
  public HttpStatusCode CopyFromLocal(string sourcePath, string targetPath, int replication = 1)
  {
      FileInfo f = new FileInfo(sourcePath);
      string urlPath = "http://" + this.namenodeHost + ":" + this.namenodePort + WEBHDFS_CONTEXT_ROOT + "/" + targetPath + "/" + f.Name + "?op=CREATE&overwrite=true&user.	name=" + this.hdfsUsername;

      HttpWebRequest req = WebRequest.Create(urlPath) as HttpWebRequest;
      req.Method = WebRequestMethods.Http.Put;
      req.UserAgent = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)";
      req.Accept = "application/json";
      req.AllowAutoRedirect = false;    // set AllowAutoRedirect to false, because we need to get the redirected path
      HttpWebResponse resp = req.GetResponse() as HttpWebResponse;


      var url = new Uri(resp.Headers["Location"]); // This has the location of redirtected URL       


      // 2) using the redirected path  

      urlPath = url.AbsoluteUri;
      long fileLength;
      fileLength = f.Length;

      //Create the request
      HttpWebRequest request = (HttpWebRequest)System.Net.HttpWebRequest.Create(urlPath);

      //Let the server know we want to "put" a file on it
      request.Method = WebRequestMethods.Http.Put;

      //Set the length of the content (file) we are sending
      request.ContentLength = fileLength;

      //*** This is required for our WebDav server ***
      request.SendChunked = true;
      request.Headers.Add("Translate: f");
      request.AllowWriteStreamBuffering = false;
      request.Timeout = System.Threading.Timeout.Infinite;

      //Send the request to the server, and get the 
      // server's (file) Stream in return.
      System.IO.Stream s = request.GetRequestStream();

      //Open the file so we can read the data from it
      System.IO.FileStream fs = new System.IO.FileStream(sourcePath, System.IO.FileMode.Open, System.IO.FileAccess.Read);

      //Create the buffer for storing the bytes read from the file
      int byteTransferRate = 1024;
      byte[] bytes = new byte[byteTransferRate];
      int bytesRead = 0;
      long totalBytesRead = 0;

      //Read from the file and write it to the server's stream.
      do
      {
          //Read from the file
          bytesRead = fs.Read(bytes, 0, bytes.Length);


          if (bytesRead > 0)
          {
              totalBytesRead += bytesRead;

              //Write to stream
              s.Write(bytes, 0, bytesRead);

          }

      } while (bytesRead > 0);

      //Close the server stream
      s.Close();
      s.Dispose();
      s = null;

      //Close the file
      fs.Close();
      fs.Dispose();
      fs = null;

      HttpWebResponse response = (HttpWebResponse)request.GetResponse();

      //Get the StatusCode from the server's Response
      HttpStatusCode code = response.StatusCode;

      //Close the response
      response.Close();
      response = null;

      //Validate the uploaded file.
      // Check the totalBytesRead and the fileLength: Both must be an exact match.
      //
      // Check the StatusCode from the server and make sure the file was "Created"
      // Note: There are many different possible status codes. You can choose
      // which ones you want to test for by looking at the "HttpStatusCode" enumerator.

      if (totalBytesRead == fileLength && code == HttpStatusCode.Created)
      {
          Console.WriteLine("The file has uploaded successfully! Upload Complete");


      }
      else
      {
          Console.WriteLine("The file did not upload successfully. Upload Failed");

      }

      return code;





  }
  
  ///<summary>
 ///Copy file from HDFS to local machine
 ///<param name="sourcePath">Location of file on HDFS, including filename and extension (FullFileName)</param>
 ///<param name="targetPath">Location of file to localhost,including filename and extension (FullFileName) </param>
 ///<returns>HttpStatusCode</returns>
 ///</summary>
  public HttpStatusCode copyToLocal(string sourcePath,string  targetPath)
  {
    // Create the final url with params
	string urlPath = "http://" + this.namenodeHost + ":" +   this.namenodePort + WEBHDFS_CONTEXT_ROOT + "/"  + sourcePath +"?op=OPEN&overwrite=true&user.name="+ this.hdfsUsername ;
    BetterWebClient wc = new BetterWebClient();
	wc.AllowAutoRedirect = true ;
	wc.DownloadFile(urlPath,targetPath);
	return wc.StatusCode() ;
	
 }
    }
}
