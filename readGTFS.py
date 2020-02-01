from zipfile import ZipFile
from io import StringIO
import pandas as pd


def main():
    with ZipFile('sample-feed.zip') as zip:
        for file in zip.filelist:
            file_name = file.filename
            with zip.open(file_name) as f:
                bytes = f.read()
                s = str(bytes, 'utf-8')
                data = StringIO(s)
                df = pd.read_csv(data)
                # df = df.transpose()
                print()
                print(file_name.split('.txt')[0])
                print(df)


if __name__ == "__main__":
    main()
