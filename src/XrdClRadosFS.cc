//------------------------------------------------------------------------------
// Copyright (c) 2014 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClPlugInInterface.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdClRadosFS.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdVersion.hh"
#include "radosfs/Filesystem.hh"
#include "radosfs/File.hh"
#include "radosfs/Dir.hh"
#include "radosfs/FsObj.hh"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept> 
#include <iostream>
#include <sstream>

#include <curl/curl.h>

XrdVERSIONINFO(XrdClGetPlugIn, XrdClRadosFS)

#define XRDCLRADOSFS_MAXSTRIPESIZE 2*1024*1024*1024ll


namespace radosfs {
  radosfs::Filesystem gRadosFs;

  //----------------------------------------------------------------------------
  // CURL Escape functions - all XATTR functions 
  // use for in and out URL escaped key/val strings
  //----------------------------------------------------------------------------
  std::string curl_escaped(const std::string &str) 
  {
    std::string ret_str="<no-encoding>";
    // encode the key
    CURL *curl = curl_easy_init();
    if(curl) {
      char *output = curl_easy_escape(curl, str.c_str(), str.length());
      if(output) {
	ret_str = output;
	curl_free(output);
      } 
    }
    return ret_str;
  }

  // curl escaping function
  std::string curl_unescaped(std::string &str) 
  {
    std::string ret_str="<no-encoding>";
    // encode the key
    CURL *curl = curl_easy_init();
    if(curl) {
      char *output = curl_easy_unescape(curl, str.c_str(), str.length(), 0);
      if(output) {
	ret_str = output;
	curl_free(output);
      } 
    }
    return ret_str;
  }

  uint64_t
  parseUnit(std::string &instring) {
    XrdOucString sizestring = instring.c_str();
    errno = 0;
    unsigned long long convfactor;
    convfactor = 1ll;
    if (!sizestring.length()) {
      errno = EINVAL;
      return 0;
    }
    if (sizestring.endswith("B") || sizestring.endswith("b")) {
      sizestring.erase(sizestring.length() - 1);
    }
    if (sizestring.endswith("E") || sizestring.endswith("e")) {
      convfactor = 1024ll * 1024ll * 1024ll * 1024ll * 1024ll * 1024ll;
    }
    if (sizestring.endswith("P") || sizestring.endswith("p")) {
      convfactor = 1024ll * 1024ll * 1024ll * 1024ll * 1024ll;
    }
    if (sizestring.endswith("T") || sizestring.endswith("t")) {
      convfactor = 1024ll * 1024ll * 1024ll * 1024ll;
    }
    if (sizestring.endswith("G") || sizestring.endswith("g")) {
      convfactor = 1024ll * 1024ll * 1024ll;
    }
    if (sizestring.endswith("M") || sizestring.endswith("m")) {
      convfactor = 1024ll * 1024ll;
    }
    if (sizestring.endswith("K") || sizestring.endswith("k")) {
      convfactor = 1024ll;
    }
    if (sizestring.endswith("S") || sizestring.endswith("s")) {
      convfactor = 1ll;
    }
    if ((sizestring.length() > 3) && (sizestring.endswith("MIN") || sizestring.endswith("min"))) {
      convfactor = 60ll;
    }
    if (sizestring.endswith("H") || sizestring.endswith("h")) {
      convfactor = 3600ll;
    }
    if (sizestring.endswith("D") || sizestring.endswith("d")) {
      convfactor = 86400ll;
    }
    if (sizestring.endswith("W") || sizestring.endswith("w")) {
      convfactor = 7 * 86400ll;
    }
    if ((sizestring.length() > 2) && (sizestring.endswith("MO") || sizestring.endswith("mo"))) {
      convfactor = 31 * 86400ll;
    }
    if (sizestring.endswith("Y") || sizestring.endswith("y")) {
      convfactor = 365 * 86400ll;
    }
    if (convfactor > 1)
      sizestring.erase(sizestring.length() - 1);

    if ((sizestring.find(".")) != STR_NPOS) {
      return ((unsigned long long) (strtod(sizestring.c_str(), NULL) * convfactor));
    } else {
      return (strtoll(sizestring.c_str(), 0, 10) * convfactor);
    }
  }
}

using namespace XrdCl;

namespace
{
  //----------------------------------------------------------------------------
  // A plugin that forwards all the calls to RadosFS
  //----------------------------------------------------------------------------
  
  class RadosFsFile: public XrdCl::FilePlugIn
  {
    public:
      //------------------------------------------------------------------------
      // Constructor
      //------------------------------------------------------------------------
      RadosFsFile()
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::RadosFsFile" );
        pFile = new File( false );
	radosFile = 0 ;
      }

      //------------------------------------------------------------------------
      // Destructor
      //------------------------------------------------------------------------
      virtual ~RadosFsFile()
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::~RadosFsFile" );
        delete pFile;
	delete radosFile;
      }

      //------------------------------------------------------------------------
      // Open
      //------------------------------------------------------------------------
      virtual XRootDStatus Open( const std::string &url,
                                 OpenFlags::Flags   flags,
                                 Access::Mode       mode,
                                 ResponseHandler   *handler,
                                 uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
	uid_t uid;
	gid_t gid;
	int retc = 0;
	uint64_t stripe_size = 0;

	radosfs::gRadosFs.getIds(&uid,&gid);
        log->Debug( 1, "RadosFsFile::Open uid=%u gid=%u", uid,gid);
	radosfs::File::OpenMode radosfsmode=radosfs::File::MODE_READ;

	XrdCl::URL xUrl(url);
	XrdCl::URL::ParamsMap cgi = xUrl.GetParams();

	XRootDStatus st; 
	if (flags & OpenFlags::Update) {
	  radosfsmode = radosfs::File::MODE_READ_WRITE;
	} else {
	  if (flags & OpenFlags::Write) {
	    radosfsmode = radosfs::File::MODE_WRITE;
	  }
	}

	if ( flags & OpenFlags::New ) { //& flags & OpenFlags::MakePath) {
	  std::string parentPath = xUrl.GetPath();
	  parentPath.erase(parentPath.rfind("/")+1);

	  log->Debug( 1, "RadosFsFile::Open mkdpath=%s", parentPath.c_str());
	  radosfs::Dir radosDir (&radosfs::gRadosFs, parentPath);
	  struct stat buf;
	  // check that the parent path exists, otherwise create it
	  if ( ( retc = radosDir.stat(&buf) ) ) {

	    if ( ( retc = radosDir.create(-1, true) ) ) {
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   -retc,
				   "radosfs error");
	    }
	  }
	}

	log->Debug( 1, "RadosFsFile::Open(radosfs::File)");
	if (!radosFile) {
	  radosFile = new radosfs::File(&radosfs::gRadosFs, xUrl.GetPath(), radosfsmode);
	  if ( (flags && OpenFlags::New) || 
	       (flags && OpenFlags::Delete) ) {
	    if (cgi.count("diamond.stripe")) {
	      stripe_size = radosfs::parseUnit(cgi["diamond.stripe"]);
	      if ( (stripe_size > 0) && (stripe_size < XRDCLRADOSFS_MAXSTRIPESIZE) ) {
		log->Debug( 1, "RadosFsFile::Open stripeSize=%llu", stripe_size);
	      } else {
		log->Debug( 1, "RadosFsFile::Open using default stripe size (0 < %llu < %llu ", stripe_size, XRDCLRADOSFS_MAXSTRIPESIZE);
	      }
	    }
	  }
	}

	if (flags & OpenFlags::Delete) {
	  log->Debug( 1, "RadosFsFile::Open(Delete)");
	  int retc = radosFile->remove();
	  // it might be that there is no file to delete, and we can ignore that
	  if (retc && retc != -ENOENT) {
	    
	    st = XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc, 
			       "radosfs error");
	    delete radosFile;
	    radosFile = 0;
	    return st;
	  }
	}
	
	if ( (flags & OpenFlags::Delete) || (flags & OpenFlags::New) ) {
	  log->Debug( 1, "RadosFsFile::Open(New)" );
	  // file should not exist
	  int retc=0;
	  if ( (retc = radosFile->create(mode,"",(size_t) stripe_size)) ) {
	    log->Debug( 1, "Failed RadosFsFile::Open retc=%d mode=%x", -retc, mode );
	    st = XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc, 
			       "radosfs error");
	    delete radosFile;
	    radosFile = 0;
	  } else {
	    XRootDStatus* ret_st = new XRootDStatus(st);
	    handler->HandleResponse(ret_st, 0);
	  }
	} else {
	  log->Debug( 1, "RadosFsFile::Open(Existing)" );
	  int retc=0;
	  struct stat buf;
	  memset((void*)(&buf), 0, sizeof(buf));

	  // file should exist
	  if ( (retc = radosFile->stat(&buf)) ) {
	    log->Debug( 1, "Failed RadosFsFile::Open retc=%d", -retc );
	    st = XRootDStatus( XrdCl::stError,
			      XrdCl::errOSError,
			      -retc,
			      "radosfs error");
	    delete radosFile;
	    radosFile = 0;
	  } else {
	    log->Debug( 1, "Opened RadosFsFile::Open" );
	    XRootDStatus* ret_st = new XRootDStatus(st);
            handler->HandleResponse(ret_st, 0);
	  }
	}
	return st;
      }

      //------------------------------------------------------------------------
      // Close
      //------------------------------------------------------------------------
      virtual XRootDStatus Close( ResponseHandler *handler,
                                  uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Close" );
	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	handler->HandleResponse(ret_st, 0); 
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // Stat
      //------------------------------------------------------------------------
      virtual XRootDStatus Stat( bool             force,
                                 ResponseHandler *handler,
                                 uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Stat" );
	int retc=0;
	struct stat buf;
	memset(&buf, 0, sizeof(struct stat));
	// file should exist
	if ( (!radosFile && (retc=-ENODEV)) || (retc = radosFile->stat(&buf)) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	log->Debug( 1, "RadosFsFile::Stat size=%lu mode=%x dev=%lu ino=%lu rdev=%lu", 
		    (unsigned long) buf.st_size, 
		    buf.st_mode, 
		    (unsigned long) buf.st_dev, 
		    (unsigned long)buf.st_ino, 
		    (unsigned long) buf.st_rdev );
	
	XRootDStatus st;
	StatInfo* sinfo = new StatInfo();
	std::ostringstream data;

	data << buf.st_dev << " " << buf.st_size<< " "
	     << buf.st_mode << " " << buf.st_mtime;
	if (!sinfo->ParseServerResponse(data.str().c_str())) {
	  delete sinfo;
	  return XRootDStatus(XrdCl::stError, errDataError);
	} else {
	  XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	  AnyObject* obj = new AnyObject();
	  obj->Set(sinfo);
	  handler->HandleResponse(ret_st, obj); 
	  return XRootDStatus( XrdCl::stOK,0,0,"");
	}
      }

      //------------------------------------------------------------------------
      // Read
      //------------------------------------------------------------------------
      virtual XRootDStatus Read( uint64_t         offset,
                                 uint32_t         size,
                                 void            *buffer,
                                 ResponseHandler *handler,
                                 uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Read" );

	int retc=0;
	ssize_t rbytes=0;
	if ( (!radosFile && (retc=-ENODEV)) || ( (rbytes=radosFile->read((char*)buffer, offset, size)) < 0) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}
 
	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	ChunkInfo* chunkInfo = new ChunkInfo(offset, rbytes, buffer);
	AnyObject* obj = new AnyObject();
	obj->Set(chunkInfo);
	handler->HandleResponse(ret_st, obj);
	return XRootDStatus( XrdCl::stOK,0,0,"");

        //return pFile->Read( offset, size, buffer, handler, timeout );
      }

      //------------------------------------------------------------------------
      // Write
      //------------------------------------------------------------------------
      virtual XRootDStatus Write( uint64_t         offset,
                                  uint32_t         size,
                                  const void      *buffer,
                                  ResponseHandler *handler,
                                  uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Write offset=%llu size=%lu" , offset, size);
	int retc=0;
	if ( (!radosFile && (retc=-ENODEV)) || (retc=radosFile->writeSync((const char*)buffer, offset, size)) ) {
	  log->Debug( 1, "Called RadosFsFile::Write retc=%lu", retc);

	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}
	log->Debug( 1, "Called RadosFsFile::Write retc=%lu", retc);

	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	handler->HandleResponse(ret_st, 0);
	return XRootDStatus( XrdCl::stOK,0,0,"");	
      }

      //------------------------------------------------------------------------
      // Sync
      //------------------------------------------------------------------------
      virtual XRootDStatus Sync( ResponseHandler *handler,
                                 uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Sync" );

	int retc=0;
	
	if ( (!radosFile && (retc=-ENODEV)) || (retc=radosFile->sync() )) {
	  log->Debug( 1, "Called RadosFsFile::Sync retc=%lu", retc);

	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	handler->HandleResponse(ret_st, 0);
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // Truncate
      //------------------------------------------------------------------------
      virtual XRootDStatus Truncate( uint64_t         size,
                                     ResponseHandler *handler,
                                     uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Truncate size=%llu" , (unsigned long long) size);
	int retc=0;
	if ( (!radosFile && (retc=-ENODEV)) || (retc=radosFile->truncate(size) )) {
	  log->Debug( 1, "Called RadosFsFile::Truncate retc=%lu", retc);
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	handler->HandleResponse(ret_st, 0);
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // VectorRead
      //------------------------------------------------------------------------
      virtual XRootDStatus VectorRead( const ChunkList &chunks,
                                       void            *buffer,
                                       ResponseHandler *handler,
                                       uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::VectorRead" );
	int retc = 0;
	ChunkList::const_iterator it;
	if ( (!radosFile && (retc=-ENODEV)) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	std::vector<radosfs::FileReadData> rvec;
	std::vector<ssize_t> frd_retc;
	std::string asyncOpId;

	char* obuffer = (char*)buffer;

	// pupulate the vector read data structure
	for (it = chunks.begin(); it != chunks.end(); ++it) {
	  frd_retc.push_back(0);
	  radosfs::FileReadData frd(it->buffer? (char*)it->buffer: (char*)obuffer, it->offset, it->length, &frd_retc.back());
	  rvec.push_back(frd);
	  obuffer += it->length;
	}
	ssize_t rbytes = radosFile->read(rvec,&asyncOpId);

	// ENOENT etc. occur instantaneously
	if (rbytes < 0) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -rbytes,
			       "radosfs error");
	}

	// TODO: use asyncOpId sync instead of global file sync
	radosFile->sync();

	// verify no vector read error
	for (size_t i = 0; i < frd_retc.size(); ++i) {
	  if (frd_retc[i] < 0) {
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 -frd_retc[i],
				 "radosfs error");
	  }
	}

	XRootDStatus* ret_st = new XRootDStatus( XrdCl::stOK,0,0,"");
	handler->HandleResponse(ret_st, 0);
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // Fcntl
      //------------------------------------------------------------------------
      virtual XRootDStatus Fcntl( const Buffer    &arg,
                                  ResponseHandler *handler,
                                  uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Fcntl" );

	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // Visa
      //------------------------------------------------------------------------
      virtual XRootDStatus Visa( ResponseHandler *handler,
                                 uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::Visa" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // IsOpen
      //------------------------------------------------------------------------
      virtual bool IsOpen() const
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::IsOpen" );
	return radosFile?true:false;
      }

      //------------------------------------------------------------------------
      // SetProperty
      //------------------------------------------------------------------------
      virtual bool SetProperty( const std::string &name,
                                const std::string &value )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::SetProperty" );
	return false;
      }

      //------------------------------------------------------------------------
      // GetProperty
      //------------------------------------------------------------------------
      virtual bool GetProperty( const std::string &name,
                                std::string &value ) const
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFile::GetProperty" );
	return false;
      }

    private:
      XrdCl::File *pFile;
      radosfs::File *radosFile;
  };

  //----------------------------------------------------------------------------
  // A plug-in that forwards all the calls to a XrdCl::FileSystem object
  //----------------------------------------------------------------------------
  class RadosFsFileSystem: public FileSystemPlugIn
  {
    public:
      //------------------------------------------------------------------------
      // Constructor
      //------------------------------------------------------------------------
      RadosFsFileSystem( const std::string &url ) : mUrl(url)
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::RadosFsFileSystem" );
        pFileSystem = new XrdCl::FileSystem( URL(url), false );
	
      }

      //------------------------------------------------------------------------
      // Destructor
      //------------------------------------------------------------------------
      virtual ~RadosFsFileSystem()
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::~RadosFsFileSysytem" );
        delete pFileSystem;
      }

      //------------------------------------------------------------------------
      // Locate
      //------------------------------------------------------------------------
      virtual XRootDStatus Locate( const std::string &path,
                                   OpenFlags::Flags   flags,
                                   ResponseHandler   *handler,
                                   uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Locate" );

	LocationInfo* response = new LocationInfo();
	LocationInfo::Location location(mUrl.GetHostId(), LocationInfo::ServerOnline, LocationInfo::ReadWrite);
	
	response->Add(location);

	XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	AnyObject* obj = new AnyObject();
	obj->Set(response);
	handler->HandleResponse(ret_st, obj); 
	log->Debug( 1, "RadosFsFileSystem::DirList returning dirlist object");
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // Mv
      //------------------------------------------------------------------------
      virtual XRootDStatus Mv( const std::string &source,
                               const std::string &dest,
                               ResponseHandler   *handler,
                               uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Mv" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // Query
      //------------------------------------------------------------------------
      virtual XRootDStatus Query( QueryCode::Code  queryCode,
                                  const Buffer    &arg,
                                  ResponseHandler *handler,
                                  uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Query %s", arg.GetBuffer() );

	std::string path = (const char*)arg.GetBuffer();
	size_t pos = path.find("&");
	path.erase(path.find("&"));

	XrdOucEnv qEnv(arg.GetBuffer()+ pos + 1);

	int retc = 0;

	if (queryCode == QueryCode::XAttr) {
	  log->Debug( 1, "RadosFsFileSystem::Query::XAttr" );

	  if (! path.length()) {
	    log->Error( 1, "RadosFsFileSystem::Query::XAttr path missing" );
	    // no path specified e.g. set,get,list,rm
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 EINVAL,
				 "no path specified to Query(XAttr)");
	  }

	  int envlen=0;

	  log->Debug( 1, "RadosFsFileSystem::Query::XAttr env='%s' op='%s'" , qEnv.Env(envlen), qEnv.Get("op"));

	  if (! qEnv.Get("op")) {
	    // no command specified e.g. set,get,list,rm
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 EINVAL,
				 "no operation specified to Query(XAttr)");
	  }

	  std::string op = qEnv.Get("op");

	  if ( op == "get" ) {

	    if ( !qEnv.Get("key") ) {
	    // no key specified
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 EINVAL,
				 "no key specified to Query(XAttr::get)");
	    }

	    std::string key = qEnv.Get("key");
	    key =  radosfs::curl_unescaped(key);
	    std::string val;

	    if ( ( retc = radosfs::gRadosFs.getXAttr(path, key, val)) < 0 ) {
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   -retc,
				   "radosfs error");
	    }

	    val =  radosfs::curl_escaped(val);
	    AnyObject* obj = new AnyObject();
	    BinaryDataInfo *data = new BinaryDataInfo();
	    data->Allocate( val.length() );
	    data->Append( val.c_str(), retc );
	    obj->Set( data );

	    XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	    handler->HandleResponse(ret_st, obj); 
	    log->Debug( 1, "RadosFsFileSystem::Query::XAttr::get returning val='%s'", val.c_str());
	    return XRootDStatus( XrdCl::stOK,0,0,"");

	  }

	  if ( op == "set" ) {

	    if ( !qEnv.Get("key") ) {
	      log->Debug( 1, "RadosFsFileSystem::Query::XAttr::set missing key");
	      // no key specified
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   EINVAL,
				   "no key specified to Query(XAttr::set)");
	    }

	    if ( !qEnv.Get("val") ) {
	      log->Debug( 1, "RadosFsFileSystem::Query::XAttr::set missing val");
	    // no value specified
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 EINVAL,
				 "no val specified to Query(XAttr::set)");
	    }

	    std::string key = qEnv.Get("key");
	    std::string val = qEnv.Get("val");
	    
	    key =  radosfs::curl_unescaped(key);
	    val =  radosfs::curl_unescaped(val);

	    log->Debug( 1, "RadosFsFileSystem::Query::XAttr::set key='%s' val='%s'", key.c_str(), val.c_str() );


	    if ( ( retc = radosfs::gRadosFs.setXAttr(path, key, val)) ) {
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   -retc,
				   "radosfs error");
	    }

	    XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");

	    AnyObject* obj = new AnyObject();
	    BinaryDataInfo *data = new BinaryDataInfo();
	    std::string rs="# setting key-value '" + key + "':='" + val + "'";
	    data->Allocate( rs.length()+1 );
	    data->Append( rs.c_str(), rs.length()+1 );
	    obj->Set( data );
	    log->Debug( 1, "RadosFsFileSystem::Query::XAttr::set returning OK");
	    handler->HandleResponse(ret_st, obj); 
	    return XRootDStatus( XrdCl::stOK,0,0,"");
	  }

	  if ( op == "list" ) {
	    std::map<std::string, std::string> xattr;

	    if ( ( retc = radosfs::gRadosFs.getXAttrsMap(path, xattr)) ) {
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   -retc,
				   "radosfs error");
	    }

	    AnyObject* obj = new AnyObject();


	    BinaryDataInfo *data = new BinaryDataInfo();
	    std::string encoded_xattr;

	    std::map<std::string, std::string>::const_iterator it;


	    for ( it = xattr.begin() ; it != xattr.end() ; ++it) {
	      encoded_xattr += radosfs::curl_escaped(it->first);
	      encoded_xattr += "\t:=:\t";
	      encoded_xattr += radosfs::curl_escaped(it->second);
	    }

	    data->Allocate( encoded_xattr.length() );
	    data->Append( encoded_xattr.c_str(), encoded_xattr.length() );
	    obj->Set( data );

	    XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	    handler->HandleResponse(ret_st, obj); 
	    log->Debug( 1, "RadosFsFileSystem::Query::XAttr::list returning map:size=%d", xattr.size());
	    return XRootDStatus( XrdCl::stOK,0,0,"");
	  }

	  if ( op == "rm" ) {

	    if ( !qEnv.Get("key") ) {
	    // no key specified
	    return XRootDStatus( XrdCl::stError,
				 XrdCl::errOSError,
				 EINVAL,
				 "no key specified to Query(XAttr:rm)");
	    }

	    std::string key = qEnv.Get("key");

	    if ( ( retc = radosfs::gRadosFs.removeXAttr(path, key)) ) {
	      return XRootDStatus( XrdCl::stError,
				   XrdCl::errOSError,
				   -retc,
				   "radosfs error");
	    }

	    XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	    handler->HandleResponse(ret_st, new AnyObject()); 
	    log->Debug( 1, "RadosFsFileSystem::Query::XAttr::rm returning OK");
	    return XRootDStatus( XrdCl::stOK,0,0,"");
	  }
	}

	// unsupported operation
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "operation not supported in Query");
	
      }

      //------------------------------------------------------------------------
      // Truncate
      //------------------------------------------------------------------------
      virtual XRootDStatus Truncate( const std::string &path,
                                     uint64_t           size,
                                     ResponseHandler   *handler,
                                     uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Truncate" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
	//        return pFileSystem->Truncate( path, size, handler, timeout );
      }

      //------------------------------------------------------------------------
      // Rm
      //------------------------------------------------------------------------
      virtual XRootDStatus Rm( const std::string &path,
                               ResponseHandler   *handler,
                               uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Rm" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // MkDir
      //------------------------------------------------------------------------
      virtual XRootDStatus MkDir( const std::string &path,
                                  MkDirFlags::Flags  flags,
                                  Access::Mode       mode,
                                  ResponseHandler   *handler,
                                  uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::MkDir path=", path.c_str() );
	int retc = 0;

	radosfs::Dir radosDir (&radosfs::gRadosFs, path);
	if ( ( retc = radosDir.create(mode) ) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	handler->HandleResponse(ret_st, new AnyObject()); 
	log->Debug( 1, "RadosFsFileSystem::Mkdir returning OK");	  
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // RmDir
      //------------------------------------------------------------------------
      virtual XRootDStatus RmDir( const std::string &path,
                                  ResponseHandler   *handler,
                                  uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::RmDir path=%s", path.c_str() );
	int retc = 0;

	radosfs::Dir radosDir (&radosfs::gRadosFs, path);
	if ( ( retc = radosDir.remove() ) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	handler->HandleResponse(ret_st, new AnyObject()); 
	log->Debug( 1, "RadosFsFileSystem::Rmdir returning OK");	  
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // ChMod
      //------------------------------------------------------------------------
      virtual XRootDStatus ChMod( const std::string &path,
                                  Access::Mode       mode,
                                  ResponseHandler   *handler,
                                  uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::ChMod path=%s", path.c_str() );
	int retc = 0;
	
	radosfs::Dir radosDir (&radosfs::gRadosFs, path);
	if ( ( retc = radosDir.chmod(mode) ) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	handler->HandleResponse(ret_st, new AnyObject()); 
	log->Debug( 1, "RadosFsFileSystem::Chmod returning OK");	  
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // Ping
      //------------------------------------------------------------------------
      virtual XRootDStatus Ping( ResponseHandler *handler,
                                 uint16_t         timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Ping" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // Stat
      //------------------------------------------------------------------------
      virtual XRootDStatus Stat( const std::string &path,
                                 ResponseHandler   *handler,
                                 uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Stat" );

	XrdCl::URL xUrl(path);

	int retc=0;
	struct stat buf;
	memset(&buf, 0, sizeof(struct stat));
	// file should exist
	if ( ( retc = radosfs::gRadosFs.stat(xUrl.GetPath(), &buf)) ) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	log->Debug( 1, "RadosFsFileSystem::Stat size=%lu mode=%x dev=%lu ino=%lu rdev=%lu", 
		    (unsigned long) buf.st_size, 
		    buf.st_mode, 
		    (unsigned long) buf.st_dev, 
		    (unsigned long)buf.st_ino, 
		    (unsigned long) buf.st_rdev );
	
	XRootDStatus st;
	StatInfo* sinfo = new StatInfo();
	std::ostringstream data;

	data << buf.st_dev << " " << buf.st_size<< " "
	     << buf.st_mode << " " << buf.st_mtime;
	if (!sinfo->ParseServerResponse(data.str().c_str())) {
	  delete sinfo;
	  return XRootDStatus(XrdCl::stError, errDataError);
	} else {
	  XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	  AnyObject* obj = new AnyObject();
 	  obj->Set(sinfo);
	  handler->HandleResponse(ret_st, obj); 
	  log->Debug( 1, "RadosFsFileSystem::Stat returning stat structure");
	  return XRootDStatus( XrdCl::stOK,0,0,"");
	}
      }

      //------------------------------------------------------------------------
      // StatVFS
      //------------------------------------------------------------------------
      virtual XRootDStatus StatVFS( const std::string &path,
                                    ResponseHandler   *handler,
                                    uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::StatVFS" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // Protocol
      //------------------------------------------------------------------------
      virtual XRootDStatus Protocol( ResponseHandler *handler,
                                     uint16_t         timeout = 0 )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Protocol" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // DirlList
      //------------------------------------------------------------------------
      virtual XRootDStatus DirList( const std::string   &path,
                                    DirListFlags::Flags  flags,
                                    ResponseHandler     *handler,
                                    uint16_t             timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::DirList" );

	std::set<std::string> entries;
	std::set<std::string>::const_iterator it;

	DirectoryList* response = new DirectoryList();
	response->SetParentName( path );

	radosfs::Dir radosDir(&radosfs::gRadosFs, path);

	radosDir.update();

	int retc = radosDir.entryList(entries);

	if (retc) {
	  return XRootDStatus( XrdCl::stError,
			       XrdCl::errOSError,
			       -retc,
			       "radosfs error");
	}

	for (it=entries.begin(); it!=entries.end(); ++it) {
	  DirectoryList::ListEntry* entry = new DirectoryList::ListEntry(mUrl.GetHostId(),*it,0);
	  response->Add(entry);

	  if( flags & DirListFlags::Stat ) {
	    struct stat buf;
	    std::string statPath = path;
	    if (*path.rbegin() != '/')
	      statPath += "/";
	    statPath += *it;

	    log->Debug( 1, "RadosFsFileSystem::DirList retrieve stat for entry '%s' (%s)", it->c_str(), statPath.c_str());
	    radosfs::FsObj* entryinfo = radosfs::gRadosFs.getFsObj( statPath );
	    if (!entryinfo->stat(&buf)) {
	      StatInfo* statInfo =  new StatInfo();	      
	      std::ostringstream sstatinfo;

	      XrdCl::StatInfo::Flags flags = (StatInfo::Flags) 0;
	      if (buf.st_mode & S_IXUSR) 
		flags = (StatInfo::Flags) ( flags | StatInfo::XBitSet );

	      if (S_ISDIR(buf.st_mode))
		flags = (StatInfo::Flags) ( flags | StatInfo::IsDir );
	      else
		if (!S_ISREG(buf.st_mode))
		  flags = (StatInfo::Flags) ( flags | StatInfo::Other );

	      if (buf.st_mode & S_IRUSR)
		flags = (StatInfo::Flags) ( flags | StatInfo::IsReadable );

	      if (buf.st_mode & S_IWUSR)
		flags = (StatInfo::Flags) ( flags | StatInfo::IsWritable );

	      sstatinfo << buf.st_ino << " " << buf.st_size << " " << flags << " " << buf.st_mtime;
	      log->Debug( 1, "RadosFsFileSystem::DirList parsing stat info %s", sstatinfo.str().c_str());
	      statInfo->ParseServerResponse(sstatinfo.str().c_str());
	      entry->SetStatInfo(statInfo);
	    } else {
	      // no stat available
	    }
	  }
	}

	XRootDStatus* ret_st = new XRootDStatus(XrdCl::stOK, 0, 0, "");
	AnyObject* obj = new AnyObject();
	obj->Set(response);
	handler->HandleResponse(ret_st, obj); 
	log->Debug( 1, "RadosFsFileSystem::DirList returning dirlist object");
	return XRootDStatus( XrdCl::stOK,0,0,"");
      }

      //------------------------------------------------------------------------
      // SendInfo
      //------------------------------------------------------------------------
      virtual XRootDStatus SendInfo( const std::string &info,
                                     ResponseHandler   *handler,
                                     uint16_t           timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::SendInfo" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
      }

      //------------------------------------------------------------------------
      // Prepare
      //------------------------------------------------------------------------
      virtual XRootDStatus Prepare( const std::vector<std::string> &fileList,
                                    PrepareFlags::Flags             flags,
                                    uint8_t                         priority,
                                    ResponseHandler                *handler,
                                    uint16_t                        timeout )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::Prepare" );
	return XRootDStatus( XrdCl::stError,
			     XrdCl::errOSError,
			     EOPNOTSUPP,
			     "radosfs error");
     }

      //------------------------------------------------------------------------
      // SetProperty
      //------------------------------------------------------------------------
      virtual bool SetProperty( const std::string &name,
                                const std::string &value )
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFileSystem::SetProperty" );
	return false;
      }

      //------------------------------------------------------------------------
      // GetProperty
      //------------------------------------------------------------------------
      virtual bool GetProperty( const std::string &name,
                                std::string &value ) const
      {
        XrdCl::Log *log = DefaultEnv::GetLog();
        log->Debug( 1, "RadosFsFilesystem::GetProperty" );
	return false;
      }

    private:
    XrdCl::FileSystem *pFileSystem;
    XrdCl::URL        mUrl;
  };
}

namespace XrdClPluginRadosFS
{
  //----------------------------------------------------------------------------
  // Constructor for plug-in 
  //----------------------------------------------------------------------------
  RadosFsFactory::RadosFsFactory( const std::map<std::string, std::string> &config ) : XrdCl::PlugInFactory() {
    // initialize the gRadosFs object with configuration from the config file
    XrdCl::Log *log = DefaultEnv::GetLog();
    log->Debug( 1, "RadosFsFactory::Constructor" );

    std::map<std::string, std::string>::const_iterator it;

    if (radosfs::gRadosFs.init(config.count("radosfs.user")?config.at("radosfs.user").c_str():"admin",
			       config.count("radosfs.config")?config.at("radosfs.config").c_str():"/etc/ceph/ceph.conf")) {
      log->Error( 1, "failed to initialize rados client");
      throw std::invalid_argument("Problem doing rados initalization.");
    } else {
      log->Info( 1, "Initialized rados client with user=%s config=%s",
		 config.count("radosfs.user")?config.at("radosfs.user").c_str():"admin",
		 config.count("radosfs.config")?config.at("radosfs.config").c_str():"/etc/ceph/ceph.conf");
      
    }

    for (it=config.begin(); it!=config.end(); ++it) {
      if (it->first.substr(0,8) == "radosfs.") {
	log->Info( 1, "Config: %s :=>: %s", it->first.c_str(), it->second.c_str() );
	// valid keys are 'config' 'datapools' 'metadatapools' 'user' 'stripe'
	if (it->first == "radosfs.datapools") {
	  std::istringstream iss(it->second);
	  do {
	    std::string prefix, pool, max_size, terminator;
	    getline(iss, prefix, ':');
	    getline(iss, pool, ':');
	    getline(iss, max_size, ':');
	    getline(iss, terminator, '|');
	    log->Dump( 1, "Parsed: prefix=%s pool=%s max-size=%s", prefix.c_str(), pool.c_str(), max_size.c_str());
	    int ret = radosfs::gRadosFs.addDataPool(pool, prefix, max_size.length()?(size_t)strtoull(max_size.c_str(),0,10):0);
	    if (ret != 0) {
	      log->Error( 1, "Error: failed to add meta data pool: %s %s", pool.c_str(), strerror(abs(ret)));
	    }

	    if (!terminator.length())
	      break;
	  } while(1);
	  
	}
	if (it->first == "radosfs.metadatapools") {
	  std::istringstream iss(it->second);
	  do {
	    std::string prefix, pool, terminator;
	    getline(iss, prefix, ':');
	    getline(iss, pool, ':');
	    getline(iss, terminator, '|');
	    log->Dump( 1, "Parsed: prefix=%s pool=%s", prefix.c_str(), pool.c_str());
	    int ret = radosfs::gRadosFs.addMetadataPool(pool, prefix);
	    if (ret != 0) {
	      log->Error( 1, "Error: failed to add meta data pool: %s %s", pool.c_str(), strerror(abs(ret)));
	    }
	    
	    if (!terminator.length())
	      break;
	  } while(1);
	}
	if (it->first == "stripesize") {
	  size_t stripesize = (size_t) strtoull(it->second.c_str(),0,10);
	  if (stripesize) {
	    log->Dump( 1, "Setting: stripesize=%lu", stripesize);
	    radosfs::gRadosFs.setFileStripeSize(stripesize);
	  }
	}
      }
    }

    // TODO: remove this
    radosfs::gRadosFs.setIds(0,0);
    radosfs::Dir rfs_root(&radosfs::gRadosFs, "/");
    rfs_root.chmod(S_IRWXU);
  }

  //----------------------------------------------------------------------------
  // Create a file plug-in for the given URL
  //----------------------------------------------------------------------------
  FilePlugIn *RadosFsFactory::CreateFile( const std::string &url )
  {
    XrdCl::Log *log = DefaultEnv::GetLog();
    log->Debug( 1, "RadosFsFactory::CreateFile" );
    return new RadosFsFile();
  }

  //----------------------------------------------------------------------------
  // Create a file system plug-in for the given URL
  //----------------------------------------------------------------------------
  FileSystemPlugIn *RadosFsFactory::CreateFileSystem( const std::string &url )
  {
    XrdCl::Log *log = DefaultEnv::GetLog();
    log->Debug( 1, "RadosFsFactory::CreateFileSystem" );
    return new RadosFsFileSystem( url );
  }
}


extern "C"
{
  void *XrdClGetPlugIn( const void *arg )
  {
    const std::map<std::string, std::string> *pconfig = static_cast <const std::map<std::string, std::string>*>(arg);
    return new XrdClPluginRadosFS::RadosFsFactory(*pconfig);
  }
}
