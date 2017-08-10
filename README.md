## sqlite_merge
A tool to merge two sqlite db files into one.

### compilation/installation
There is one source file mergedbs.c. Download a sqlite amalgmation
file, append the code in mergedbs.c to sqlite3.c, compile and install
as usual.

There is a script called install.sh which does all of the above.
Check the CFLAGS and LIBS in install.sh before invoking.
It has been tested on a ubuntu installation.

### invocation from python3

```python
from ctypes import CDLL, create\_string\_buffer
import sys

def cpdb(src, dest):
    """
    Replicate tables/indices from src to dest
    """
    errlen = 1024
    errstr = create_string_buffer(errlen)
    lib = CDLL('libsqlite3.so')
    rc = lib.md_mergedbs(
        src.encode('ascii'),
        dest.encode('ascii'),
        errstr, errlen)
    print("returned code is {}".format(rc))
    if (rc != 0):
        print("The error string is {}".format(errstr.value.decode('ascii')),
              file=sys.stderr)
```

The entry point in mergedbs.c is the function md\_mergedbs which is
what the above python code is calling.
If the function succeeds then all the tables and indexes in the file
pointed to by src will have been copied over to the file pointed to 
by dest. The original content of dest will still be there.

### caveats
1. Only been tested for the case where there is only one process 
writing to the destination database.
