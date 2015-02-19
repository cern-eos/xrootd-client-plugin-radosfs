xrootd-client-plugin-radosfs
============================
An XRootD 4 client-side plug-in for RadosFS.

Warning
-------
The source tree includes two private header files of the XrdCl source tree and needs to be kept in sync with the XRootD version source used!

URL Stripesize Options
----------------------
Stripe size can be set on the fly using an additional CGI tag "?diamond.stripe=16M" (default).

Extended Attribute API
----------------------
Since XRootD does not provide a useful API for standard FS extended attribute handling the 'query' plug-in functionality of XRootD is used to provide an extended attribute interface.

To retrieve an extended attribute:  
```                                  
xrdfs query xattr <path>?op=get&key=<URI-encoded key>
```

To set an extended attribute:       
```
xrdfs query xattr <path>?op=set&key=<URI-encoded key>&val=<URI-encoded val>
```

To list extended attributes:        
```
xrdfs query xattr <path>?op=list ( prints URI-encoded key-val pairs )
```

To delete extended attributes:      
```
xrdfs query xattr <path>?op=rm&key=<URI-encoded key>
```

The plug-in provides an escaping & unescaping interface in the radosfs namespace:

```
radosfs::curl_escape(std::string &s);
radosfs::curl_unescape(std:;string &s);
```

Plug-in Configuration
---------------------
The plug-in is configured via a plug-in configuration entry in /etc/xrootd/client.plugins.d/<name>.conf

An example configuration to delegete all IO to radosfs.cern.ch to this plug-in and store data in the 'data' 
and meta-data in the 'metadata' in RADOS with a default stripe size of 16M is given here:

```
url = root://radosfs.cern.ch:1094
lib = /usr/lib64/libXrdClRadosFS.so
enable = true

radosfs.config = /etc/ceph/ceph.conf
radosfs.datapools = /:data:2400
radosfs.metadatapools = /:metadata
radosfs.user = admin
radosfs.stripe = 16777216
```

