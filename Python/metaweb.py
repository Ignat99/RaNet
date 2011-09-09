#
# metaweb.py: A python module for writing Metaweb-enabled applications
#
"""
This module defines classes for working with Metaweb databases.

  metaweb.Session: represents a connection to a database
  metaweb.ServiceError: exception raised by Session methods

Typical usage:

    import metaweb
    freebase = metaweb.Session("api.freebase.com")
    q1 = [{ 'type':'/music/album', 'artist':'Bob Dylan', 'name':None }]
    q2 = [{ 'type':'/music/album', 'artist':'Bruce Springsteen', 'name':None }]
    bob,bruce = freebase.read(q1, q2)  # Submit two queries, get two results
    for album in bob: print album['name']

    # Get query results with a generator method instead
    albums = freebase.results(q2)
    albumnames = (album['name'] for album in albums)
    for name in albumnames: print name

    # Download an image of U2
    result = freebase.read({"id":"/en/u2","/common/topic/image":[{"id":None}]})
    imageid = result["/common/topic/image"][0]["id"]
    data,type = freebase.download(imageid)
    print "%s image, %d bytes long" % (type, len(data))

"""

import urllib        # URL encoding
import simplejson    # JSON serialization and parsing
import urllib2       # URL content fetching
import cookielib     # HTTP Cookie handling

# Metaweb read services
READ = '/api/service/mqlread'    # Path to mqlread service
SEARCH = '/api/service/search'   # Path to search service
DOWNLOAD = '/api/trans/raw'      # Path to download service
BLURB = '/api/trans/blurb'       # Path to document blurb service
THUMB = '/api/trans/image_thumb' # Path to image thumbnail service

# Metaweb write services
LOGIN = '/api/account/login'     # Path to login service
WRITE = '/api/service/mqlwrite'  # Path to mqlwrite service
UPLOAD = '/api/service/upload'   # Path to upload service
TOUCH = '/api/service/touch'     # Path to touch service

# Metaweb services return this code on success
OK = '/api/status/ok'            

class Session(object):
    """
    This class represents a connection to a Metaweb database.

    It defines methods for submitting read, write and search queries to the
    database and methods for uploading and downloading binary data.  
    It encapsulates the database URL (hostname and port), read and write
    options, and maintains authentication and cache-related cookies.

    The Session class defines these methods:

      read(): issue one or more MQL queries to mqlread
      results(): a generator that performs a MQL query using a cursor
      search(): invoke the search service
      download(): retrieve content with trans/raw
      contenURL(): like download(), but just return the URL
      blurb(): retrieve a document blurb with trans/blurb
      blurbURL(): like blurb(), but just return the URL
      thumbnail(): retrieve an image thumbnail with trans/image_thumb
      thumbnailURL(): like thumbnail(), but just return the URL
      login(): establish credentials (as a cookie) for writes
      write(): invoke mqlwrite
      upload(): upload content
      touch(): get a fresh mwLastWriteTime cookie to defeat caching

    Each Session instance has these read/write attributes:

      host: the hostname (and optional port) of the Metaweb server as a string.
         The default is sandbox-freebase.com.  Every Monday, the sandbox is
         erased and it is updated with a fresh copy of data from
         www.freebase.com.  This makes it an ideal place to experiment.

      cookiejar: a cookielib.CookieJar object for storing cookies.
         If none is passed when the class is created, a
         cookielib.FileCookieJar is automatically created.  Note that cookies
         are not automatically loaded into or saved from this cookie jar,
         however. Clients that want to maintain authentication or cache state
         across invocations must save and load cookies themselves.

      options: a dict mapping option names to option values. Key/value
         pairs in this dict are used as envelope or URL parameters by
         methods that need them.  The read() method looks for a lang
         option, for example and the image_thumb looks for a maxwidth
         option. Options may be passed as named parameters to the Session() 
         constructor or to the various Session methods.
    """

    def __init__(self, host="sandbox-freebase.com", cookiejar=None, **options):
        """Session constructor method"""
        self.host = host
        self.cookiejar = cookiejar or cookielib.FileCookieJar()
        self.options = options

    def read(self, *queries, **options):
        """
        Submit one or more MQL queries to a Metaweb database, using any
        named options to override the option defaults. If there is
        a single query, return the results of that query. Otherwise, return
        an array of query results. Raises ServiceError if there were problems
        with any of the queries.
        """

        # How many queries are we handling?
        n = len(queries)

        # Gather options that apply to these queries
        opts = self._getopts("lang", "as_of_time", "escape",
                             "uniqueness_failure", **options)

        # Create an outer envelope object
        outer = {}

        # Build the inner envelope for each query and put it in the outer.
        for i in range(0, n):
            inner = {'query': queries[i]} # Inner envelope holds a query.
            inner.update(opts)            # Add envelope options.
            outer['q%d' % i] = inner      # Put inner in outer with name q(n).

        # Convert outer envelope to a string
        json = self._dumpjson(outer)

        # Encode the query string as a URL parameter and create a url
        urlparam = urllib.urlencode({'queries': json}) 
        url = 'http://%s%s?%s' % (self.host, READ, urlparam)
                
        # Fetch the URL contents, parse to a JSON object and check for errors.
        # From here on outer and inner refer to response, not query, envelopes.
        outer = self._check(self._fetch(url))

        # Extract results from the response envelope and return in an array.
        # If any individual query returned an error, raise a ServiceError.
        results = []
        for i in range(0, n):
            inner = outer["q%d" % i]         # Get inner envelope from outer
            self._check(inner)               # Check inner for errors
            results.append(inner['result'])  # Get query result from inner

        # If there was just one query, return its results.  Otherwise
        # return the array of results
        if n == 1:
            return results[0]
        else:
            return results

    def results(self, query, **options):
        """
        A generator version of the read() method. It accepts a single
        query, and yields query results one by one. It uses the envelope
        cursor parameter to return a full set of results even when more
        than one invocation of mqlread is required.
        """

        # Gather options that apply to this query
        opts = self._getopts("lang", "as_of_time", "escape", 
                             "uniqueness_failure", **options)

        # Build the query envelope
        envelope = {'query': query}
        envelope.update(opts)

        # Start with cursor set to true
        cursor = True

        # Loop until cursor is no longer true
        while cursor:
            # Use the cursor as an envelope parameter
            envelope['cursor'] = cursor

            # JSON-encode the envelope and convert it to a URL parameter
            params=urllib.urlencode({'query': self._dumpjson(envelope)}) 
                
            # Build the URL
            url = 'http://%s%s?%s' % (self.host, READ, params)

            # Fetch and parse the URL contents, raising ServiceError on errors
            response = self._check(self._fetch(url))
                
            # Get the results array and yield one result at a time
            results = response['result']
            for r in results:
                yield r

            # Get the new value of the cursor for the next iteration
            cursor = response['cursor']
            

    def search(self, query, **options):
        """
        Invoke the search service for the specified query string.  If that
        string ends with an asterisk, perform a prefix search instead of a
        straight query.  type, domain, type_strict, and other search service
        options may be specified as Session options or may be passed as named
        parameters.
        """
        opts = self._getopts("domain", "type", # Build a dict of search options
                             "type_strict",    # from these session options
                             "limit", "start",  
                             "escape", "mql_output",
                             **options)        # plus any passed to this method

        if query.endswith('*'):            # If search string ends with *
            opts["prefix"] = query[0:-1]   # then this is a prefix search
        else:                              # Otherwise...
            opts["query"] = query          # It is a regular query

        params = urllib.urlencode(opts)                    # Encode options
        url = "http://%s%s?%s" % (self.host,SEARCH,params) # Build URL
        envelope = self._fetch(url)                        # Fetch response
        self._check(envelope)                              # Check that its OK
        return envelope["result"]                          # Return result


    def download(self, id):
        """
	Return the content and type of the content object identified by id.

        Returns two values: the downloaded content (as a string of characters
        or bytes) and the type of that content (as a MIME-type string, from
        the Content-Type header returned by the Metaweb server). Raises
        ServiceError if the request fails with a useful message; otherwise
        raises urllib2.HTTPError.  See also the contentURL() method.
        """
        return self._trans(self.contentURL(id))


    def blurb(self, id, **options):
        """
	Return the content and type of a document blurb.  See blurbURL().
        """
        return self._trans(self.blurbURL(id, **options))

    def thumbnail(self, id, **options):
        """
	Return the content (as a binary string) and type of an image 
	thumbnail.  See thumbnailURL().
        """
        return self._trans(self.thumbnailURL(id, **options))


    def contentURL(self, id):
        """
        Return the /api/trans URL of the /type/content, /common/image, 
	or /common/document content identified by the id argument. 
	"""
        return self._transURL(id, DOWNLOAD)

    def blurbURL(self, id, **options):
        """
	Return the /api/trans URL of a blurb of the document identified by id.

        The id must refer to a /type/content or /common/document object.
        Blurb length and paragraph breaks are controlled by maxlength and
        break_paragraph options, which can be specified in the Session object
        or passed to this method.
	"""
        return self._transURL(id, BLURB, ["maxlength", "break_paragraphs"],
                              options)

    def thumbnailURL(self, id, **options):
        """
	Return the URL of a thumbnail of the image identified by id.

        The id must refer to a /type/content or /common/image object.
        Thumbnail width and height are controlled by the maxwidth and
        maxheight options, which can be specified on the Session object, or
        passed to this method.
	"""
        return self._transURL(id, THUMBNAIL, ["maxwidth","maxheight"], options)


    # A utility method that returns a dict of options.  
    # It first builds a dict containing only the specified keys and their
    # values, and only if those keys exist in self.options. 
    # Then it augments this dict with the specified options.
    def _getopts(self, *keys, **local_options):
        o = {}
        for k in keys:
            if k in self.options:
                o[k] = self.options[k]
        o.update(local_options)
        return o

    # Fetch the contents of the requested HTTP URL, handling cookies from
    # the cookie jar.  Return a tuple of http status, headers and
    # response body. This is the only method in this module that performs
    # HTTP or manages cookies.  This implementation uses the urllib2 library.
    # You can subclass and override this method if you want to use a
    # different implementation with different performance characteristics.
    def _http(self, url, headers={}, body=None):
        # Store the url in case we need it later for error reporting.
        # Note that this is not safe if multiple threads use the same Session.
        self.lasturl = url;

        # Build the request.  Will use POST if a body is supplied
        request = urllib2.Request(url, body, headers)

        # Add any cookies in the cookiejar to this request
        self.cookiejar.add_cookie_header(request)

        try:
            stream = urllib2.urlopen(request) # Try to open the URL, get stream
            self.cookiejar.extract_cookies(stream, request) # Remember cookies
            headers = stream.info()
            body = stream.read()
            return (stream.code, headers, body)
        except urllib2.HTTPError, e:          # If we get an HTTP error code
            return (e.code, e.info(), e.read())  # But return values as above
       

    # Parse a string of JSON text and return an object, or raise
    # InternalServiceError if the text is unparseable.
    # This implementation uses the simplejson library.
    # You can override it in a subclass if you want to use something else.
    def _parsejson(self, s):
        try: 
            return simplejson.loads(s)
        except:
            # If we couldn't parse the response body, then we probably have an 
            # low-level HTTP error with no JSON in its response. This should
            # not happen, but if it does, we createa a fake response object
            # so that we can raise a ServiceError as we do elsewhere.
            raise InternalServiceError(self.lasturl, s)

    # Encode the object o as JSON and return the encoded text.
    # If pretty is True, use line breaks and indentation to make the output
    # more human-readable.  Override this method if you want to use an
    # implementation other than simplejson.
    def _dumpjson(self, o, pretty=False):
        if pretty:
            return simplejson.dumps(o, indent=4)
        else:
            return simplejson.dumps(o)

    # An internal utility function to fetch the contents of a Metaweb service
    # URL and parse the JSON results and return the resulting object.
    #
    # Metaweb services normally return JSON response bodies even when an HTTP 
    # error occurs, and this function parses and returns those error objects. 
    # It only raises an error on very low-level HTTP errors that do 
    # not include a JSON object as its body.
    def _fetch(self, url, headers={}, body=None):
        # Fetch the URL contents
        (status, headers, body) = self._http(url, headers, body);
        # Parse the response body as JSON, and return the resulting object.
        return self._parsejson(body)

    # This is a utility method used by download(), blurb() and thumbnail()
    # to fetch the content and type of a specified URL, performing 
    # cookie management and error handling the way the _fetch function does.
    # Unlike other Metaweb services, trans does not normally return a JSON 
    # object, so we cannot just use _fetch here.
    def _trans(self, url):
        (status,headers,body) = self._http(url)   # Fetch url content
        if (status == 200):                       # If successful
            return body,headers['content-type']   # Return content and type
        else:                                     # HTTP status other than 200
            errobj = self._parsejson(body)        # Parse the body
            raise ServiceError(url, errobj)       # And raise ServiceError

    # An internal utility function to check the status code of a Metaweb
    # response envelope and raise a ServiceError if it is not okay.
    # Returns the response if no error.
    def _check(self, response):
        code = response['code']
        if code != OK:
            raise ServiceError(self.lasturl, response)
        else:
            return response

    # This utility method returns a URL for the trans service
    def _transURL(self, id, service, option_keys=[], options={}):
        url = "http://" + self.host + service + id    # Base URL.
        opts = self._getopts(*option_keys, **options) # Get request options.
        if len(opts) > 0:                             # If there are options...
            url += "?" + urllib.urlencode(opts)       # encode and add to url.
        return url


class ServiceError(Exception):
    """
    This exception class represents an error from a Metaweb service.

    When anything goes wrong with a Metaweb service, it returns a response
    object that includes an array of message objects.  When this occurs we
    wrap the entire response object in a ServiceError exception along
    with the URL that was requested.
    
    A ServiceError exception converts to a string that contains the
    requested URL (minus any URL parameters that contain the actual
    query details) plus the status code and message of the first (and
    usually only) message in the response. 

    The details attribute provides direct access to the complete response 
    object. The url attribute provides access to the full url.
    """

    # This constructor expects the URL requested and the parsed response.
    def __init__(self, url, details):
        self.url = url
        self.details = details

    # Convert to a string by printing url + the first error code and message
    def __str__(self):
        prefix = self.url.partition('?')[0]
        msg = self.details['messages'][0]
        return prefix + ": " + msg['code'] + ": " + msg['message']


class InternalServiceError(ServiceError):
    """
    A ServiceError with a fake response object. We raise one of these when
    we get an error so low-level that the HTTP response body is not a
    JSON object.  In this case we basically just report the HTTP error code.
    An exception of this type probably indicates a bug in this module.
    """
    def __init__(self, url, body):
        ServiceError.__init__(self, url, 
                              {'code':'Internal service error',
                               'messages':[{'code':'Unparseable response',
                                            'message':body}]
                               })
