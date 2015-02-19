FIND_PATH(RADOS_INCLUDE_DIR FsObj.hh
  HINTS
  $ENV{RADOSFS_ROOT}
  /usr
  /opt/radosfs/
  PATH_SUFFIXES include/radosfs
  PATHS /opt/radosfs
)

FIND_LIBRARY(RADOS_LIB libradosfs radosfs
  HINTS
  /usr
  /opt/radosfs/
  PATH_SUFFIXES lib
  lib64
  lib/radosfs/
  lib64/radosfs/
)

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( RadosFS DEFAULT_MSG
                                        RADOSFS_LIB
                                        RADOSFS_INCLUDE_DIR )
