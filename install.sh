#! /bin/bash

scriptdir=$(dirname $(realpath $0))
keyname=sqlite-autoconf-3190300
sqlite_url=https://www.sqlite.org/2017/${keyname}.tar.gz

cd ${scriptdir}
rm -Rf ${keyname} || true
curl ${sqlite_url} > ${keyname}.tar.gz
tar -xzf ${keyname}.tar.gz
rm -f ${keyname}.tar.gz

## Edit as required
CFLAGS=-Os
LIBS=

cd ${keyname}
cat ../mergdbs.c >> ./sqlite3.c
CFLAGS="${CFLAGS}" LIBS="${LIBS}" ./configure
make
sudo make install
cd ..
rm -Rf ${keyname} || true


# The following might be needed for the newly installed library
# to become the default one for the system
# sudo ldconfig
