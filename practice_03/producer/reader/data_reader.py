import pandas as pd
from pathlib import Path


class Reader:
    def __init__(self, file_name):
        self.file_name = file_name
        print("self.file_name", self.file_name)

        self.data = pd.DataFrame()
        self.read_file()
        self.process_data()

    def read_file(self):
        print("cwd", str(Path.cwd()))
        self.data = pd.read_csv(
            str(Path.cwd()) + "\\producer\\" + self.file_name,
            sep=",",
            header="infer",
            encoding="iso-8859-1",
        )

    def process_data(self):
        columns_tmp = self.data.columns
        columns_tmp = [column_name.lower() for column_name in columns_tmp]
        print(columns_tmp)
        self.data.columns = columns_tmp
        self.data.rename(
            columns={
                "open": "open_price",
                "close": "close_price",
                "adj close": "adj_close",
            },
            inplace=True,
        )


if __name__ == "__main__":
    pass
