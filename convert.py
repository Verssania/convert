import argparse
from os import path
import pandas as pd
import pyarrow.parquet as pq

# init constants
EXT_CSV = "csv"
EXT_PARQUET = "parquet"
HELP_MESSAGE = "Use command below for show to help:\n\tpython convert.py -h"


def main():
    args = get_parser_arguments()
    src, dst = get_src_dst(args)
    process_operations(args, src, dst)


def get_parser_arguments():
    parser = argparse.ArgumentParser(description='File conversion')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--parquet2csv",
                       action="store_true",
                       help=f'<src-filename>.{EXT_PARQUET} <dst-filename>.{EXT_CSV} -> parquet to CSV conversion')
    group.add_argument("--csv2parquet",
                       action="store_true",
                       help=f'<src-filename>.{EXT_CSV} <dst-filename>.{EXT_PARQUET} -> csv to parquet conversion')
    group.add_argument("--get_schema",
                       action="store_true",
                       help=f'<filename>.{EXT_PARQUET} -> get scheme of file')
    parser.add_argument('files',
                        nargs='*',
                        help='<src-filename> [and <dst-filename>] must be specified file names after the argument')

    return parser.parse_args()


# init source and destination files
def get_src_dst(args):
    files = args.files
    files_to_init = [None, None]  # mean [src, dst]

    count_files = 0
    if args.parquet2csv or args.csv2parquet:
        count_files = 2
    elif args.get_schema:
        count_files = 1

    if len(files) == count_files:
        for i in range(len(files)):
            files_to_init[i] = files[i]
    else:
        print("Invalid count of files")

    return files_to_init


def process_operations(args, src, dst):
    if args.parquet2csv:  # convert from parquet to csv
        convert_parquet_to_csv(src, dst)
    elif args.csv2parquet:  # convert from csv to parquet
        convert_csv_to_parquet(src, dst)
    elif args.get_schema:  # show parquet file schema
        schema = get_parquet_schema(src)
        print(f"Schema: {schema}")
    else:  # if command not supporting
        print('Operation not specified or supported\n'
              + HELP_MESSAGE)


def convert_parquet_to_csv(src, dst):
    ext_src, ext_dst = EXT_PARQUET, EXT_CSV

    if files_validation_and_preparation(src, dst, ext_src, ext_dst):
        print("Started conversion from csv to parquet..")

        df = pd.read_parquet(src)
        df.to_csv(dst)

        print(f"Completed! Destination file: {dst}")


def convert_csv_to_parquet(src, dst):
    ext_src, ext_dst = EXT_CSV, EXT_PARQUET

    if files_validation_and_preparation(src, dst, ext_src, ext_dst):
        print("Started conversion from csv to parquet..")

        df = pd.read_csv(src)
        df.to_parquet(dst)

        print(f"Completed! Destination file: {dst}")


def get_parquet_schema(src):
    ext_src = EXT_PARQUET

    if file_validation(src, ext_src):
        print(f"Source file: {src}")
        return pq.ParquetFile(src).schema_arrow


# validate extension of file
def is_extension_match(file_name, extension):
    return file_name.endswith(extension)


def file_validation(file, ext):
    return files_validation_and_preparation(file, None, ext, None)


def files_validation_and_preparation(src, dst, ext_src, ext_dst):
    # validate extension of filenames
    if not is_extension_match(src, ext_src) or\
            (dst is not None and not is_extension_match(dst, ext_dst)):
        print('Invalid extension files\n' + HELP_MESSAGE)
        return False

    # validate source file
    if not path.isfile(src):
        print(f"File {src} doesn't exist")
        return False

    # preparation destination file
    if dst is not None and not path.isfile(dst):
        print(f"File {dst} will be create!")
        dst_file = open(dst, "w+")
        dst_file.close()

    return True


if __name__ == '__main__':
    main()
