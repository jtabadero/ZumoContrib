SQL Server and SQL Compact Stores for Microsoft Azure Mobile Services
===========

This project contains the two sample offline stores for use with Microsoft Azure Mobile Services Offline Sync. 

These sample local store implementations can be used as an alternative to using SQLite especially for desktop applications (WPF, WinForms) that would rather use SQLCE or SQL Server-based stores.

To use SQLite, you would normally open the local store like this:
```sh
  var store = new MobileServiceSQLiteStore("localsync.db");
```

MobileServiceSqlCeStore 
----
- for use with SQL Compact (SQL CE)

To use SQL CE as an offline store, use the line below instead:
```sh
  var store = new MobileServiceSqlCeStore(@"c:\<yoursqlcompactdbhere>.sdf");
```

Note: If the SQL CE database doesn't exists, it will automatically be created.  


MobileServiceSqlStore
----

 - for use with SQL flavors (SQL Server/Express/LocalDb/AzureSql)

To use SQL Server-based local store, use the line below:

```sh
  var store = new MobileServiceSqlStore(@"Data Source=<yourserver>;Initial Catalog=<yourdb>;Integrated Security=SSPI;");
```  
  
Note: Unlike the SQLite and SQLCE local stores, the SQL Server Database must already exists.


