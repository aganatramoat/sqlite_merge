## sqlite_merge
A tool to merge two sqlite db files into one.

### compilation/installation
There is one source file mergedbs.c. Download a sqlite amalgmation
file, append the code in mergedbs.c to sqlite3.c, compile and install
as usual.

There is a script called install.sh which has been tested on a ubuntu 
machine. Check the CFLAGS and LIBS in install.sh before invoking.
