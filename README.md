## The converter (csv to parquet, parquet to csv)

### Tasks of the converter:
1. Converts files from csv format to parquet format;
2. Converts filed from parquet format to csv format;
3. Get the parquet schema of the parquet file;

## Requirements

```
pip install pyarrow
pip install pandas
pip install fastparquet
```

## Usage util:
python  convert.py [-h] [--parquet2csv | --csv2parquet | --get_schema] [files ...]

## Arguments

arguments | parameters | description 
:--------:|:----------:|:-----------:
-h, --help | |show this help message and exit
--parquet2csv | <src-filename>.parquet, <dst-filename>.csv | parquet to CSV conversion
--csv2parquet | <src-filename>.csv, <dst-filename>.parquet | csv to parquet conversion 
--get_schema | <filеname>.parquet | get scheme of file