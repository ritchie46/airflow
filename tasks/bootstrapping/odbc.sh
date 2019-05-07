#!/usr/bin/env bash
set -e
set -x
# https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-RHEL-or-Centos

sudo yum install -y gcc-c++ unixODBC-devel python36-devel.x86_64
sudo pip-3.6 install -U pyodbc

cd $HOME

echo install the unixODBC driver...
# note, adding "--enable-stats=no" here is not specified by Microsoft
export CPPFLAGS="-DSIZEOF_LONG_INT=8"
./configure --prefix=/usr --libdir=/usr/lib64 --sysconfdir=/etc --enable-gui=no --enable-drivers=no --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --enable-stats=no 1> configure_std.log 2> configure_err.log
make 1> make_std.log 2> make_err.log
sudo make install 1> makeinstall_std.log 2> makeinstall_err.log

# the Microsoft driver expects unixODBC to be here /usr/lib64/libodbc.so.1,
# so add soft links to the '.so.2' files
cd /usr/lib64
sudo ln -s libodbccr.so.2   libodbccr.so.1
sudo ln -s libodbcinst.so.2 libodbcinst.so.1
sudo ln -s libodbc.so.2     libodbc.so.1

echo verify installation...
echo the following commands should return information:
sudo ls -l /usr/lib64/libodbc*
odbc_config --version --longodbcversion --cflags --ulen --libs --odbcinstini --odbcini
odbcinst -j
isql --version

echo "download Microsoft ODBC Driver for Linux..."
cd $HOME
curl https://download.microsoft.com/download/B/C/D/BCDD264C-7517-4B7D-8159-C99FC5535680/RedHat6/msodbcsql-11.0.2270.0.tar.gz -o msodbcsql-11.0.2270.0.tar.gz
tar -xvz -f msodbcsql-11.0.2270.0.tar.gz
cd msodbcsql-11.0.2270.0
sudo ./install.sh install --accept-license --force 1> install_std.log 2> install_err.log

echo "Check the msodbc installation with the following commands. They should all return information that can be verified as correct..."
sudo ls -l /opt/microsoft/msodbcsql/lib64/
sudo dltest /opt/microsoft/msodbcsql/lib64/libmsodbcsql-11.0.so.2270.0 SQLGetInstalledDrivers
cat /etc/odbcinst.ini   # should contain a section called [ODBC Driver 11 for SQL Server]

echo "create dns file..."
cd $HOME
echo """[SQLDNS]
Driver      = ODBC Driver 11 for SQL Server
Description = My MS SQL Server
Trace       = No
Server      = enxdbsdatalakeftdprd.database.windows.net
""" > dnsfile

echo register the SQL Server database DSN information in /etc/odbc.ini
sudo odbcinst -i -s -f ./dnsfile -l

echo DNS databases...
cat /etc/odbc.ini   # should contain a section called [SQLDNS]

# To connect:
# python3 -c "import pyodbc; print(pyodbc.connect('DSN=SQLDNS;UID=<user>;PWD=<password>;database=<db>'))"