#
# XrdClRadosFS plug-in - An XRootD client plug-in based in libradosfs
#
# Copyright (C) 2014 CERN, Switzerland
#
# Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License at http://www.gnu.org/licenses/lgpl-3.0.txt
# for more details.

find_package(XRootD REQUIRED)
#-------------------------------------------------------------------------------
# The XrdCl lib
#-------------------------------------------------------------------------------

include_directories ( ${XROOTD_INCLUDE_DIR} ./)

add_library(
  XrdClRadosFS-${PLUGIN_VERSION}
  SHARED
  XrdClRadosFS.cc             XrdClRadosFS.hh
)

target_link_libraries(
  XrdClRadosFS-${PLUGIN_VERSION}
  XrdCl
  XrdUtils
  radosfs
  curl
)
  
set_target_properties(
  XrdClRadosFS-${PLUGIN_VERSION}
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES ""
)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS XrdClRadosFS-${PLUGIN_VERSION}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )

