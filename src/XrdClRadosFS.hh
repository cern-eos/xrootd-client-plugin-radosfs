//------------------------------------------------------------------------------
// Copyright (c) 2014 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters andreas.joachim.peters@cern.ch>
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

#ifndef __XRDCLTESTS_IDENTITY_PLUGIN_HH__
#define __XRDCLTESTS_IDENTITY_PLUGIN_HH__

#include <stdio.h>
#include "radosfs/Filesystem.hh"
#include "XrdCl/XrdClPlugInInterface.hh"

extern XrdCl::Log XrdClRFSLog;

namespace XrdClPluginRadosFS
{
  //----------------------------------------------------------------------------
  // Plugin factory
  //----------------------------------------------------------------------------
  class RadosFsFactory: public XrdCl::PlugInFactory
  {
    public:
      //------------------------------------------------------------------------
      // Constructore
      //------------------------------------------------------------------------
      RadosFsFactory(const std::map<std::string, std::string> &config);
      //------------------------------------------------------------------------
      // Destructor
      //------------------------------------------------------------------------
    virtual ~RadosFsFactory() {}

      //------------------------------------------------------------------------
      // Create a file plug-in for the given URL
      //------------------------------------------------------------------------
      virtual XrdCl::FilePlugIn *CreateFile( const std::string &url );

      //------------------------------------------------------------------------
      // Create a file system plug-in for the given URL
      //------------------------------------------------------------------------
      virtual XrdCl::FileSystemPlugIn *CreateFileSystem( const std::string &url );    

  private:
  };
};

#endif // __XRDCLTESTS_IDENTITY_PLUGIN_HH__
