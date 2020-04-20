using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

namespace SharpHadoop.Utilities.Net
{
    ///<summary>
    /// http://stackoverflow.com/questions/3574659/how-to-get-status-code-from-webclient
    /// Additions: Expose property AllowAutoRedirect
    ///</summary>
    public class BetterWebClient : WebClient
    {
        private WebRequest _Request = null;
        private bool _AllowAutoRedirect { get; set; }

        public bool AllowAutoRedirect
        {
            get { return _AllowAutoRedirect; }
            set
            {
                _AllowAutoRedirect = value;

            }
        }

        protected override WebRequest GetWebRequest(Uri address)
        {
            this._Request = base.GetWebRequest(address);

            if (this._Request is HttpWebRequest)
            {
                ((HttpWebRequest)this._Request).AllowAutoRedirect = this._AllowAutoRedirect;
            }

            return this._Request;
        }

        public HttpStatusCode StatusCode()
        {
            HttpStatusCode result;

            if (this._Request == null)
            {
                throw (new InvalidOperationException("Unable to retrieve the status  code, maybe you haven't made a request yet."));
            }

            HttpWebResponse response = base.GetWebResponse(this._Request)
                                       as HttpWebResponse;

            if (response != null)
            {
                result = response.StatusCode;
            }
            else
            {
                throw (new InvalidOperationException("Unable to retrieve the status   code, maybe you haven't made a request yet."));
            }

            return result;
        }
    }	
}
