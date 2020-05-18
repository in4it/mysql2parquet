# MySQL to parquet
Converts the output of a MySQL query to parquet

## Usage mysql2parquet
```
Usage of ./mysql2parquet:
  -compression string
        compression to apply (snappy/bzip/gzip) (default "none")
  -connectionString string
        MySQL connectionstring
  -debug string
        enable debug (default "no")
  -out string
        outputfile
  -query string
        query
```

## Usage parquetreader
```
Usage of ./parquetreader:
  -column string
        column to read
  -filename string
        input to read
```

## Examples
Connect to MySQL with login root and password secret on 127.0.0.1. Select everything from table test on database test. Write the output to filename.parquet
```
./mysql2parquet -connectionString "root:secret@tcp(127.0.0.1:3306)/test" -query "select * from test" -out filename.parquet -debug true
```

Read column "Id" from a parquet file:
```
./parquetreader -filename filename.parquet -column Id
```