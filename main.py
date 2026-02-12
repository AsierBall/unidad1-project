from pathlib import Path
from unidad1_project import CSVReader,CSVReaderPandas, TransformerMissing, WriterCsv


def main():
    # print("Hello from unidad1-project!")
    file_path = Path("./data/netflix_titles.csv")
    file_path_wr = Path("./data/netflix_titles_wr.csv")
    reader = CSVReaderPandas(file_path, 5)
    reader_generator = reader.read()
    # print(type(next(reader_generator)))
    # print([next(reader_generator) for _ in range(3)])
    # transformer = TransformerMissing(reader_generator)
    # transformer.transform()
    writer = WriterCsv(file_path_wr)
    writer.write(next(reader_generator))


if __name__ == "__main__":
    main()
