from pathlib import Path
from unidad1_project import CSVReader

def main():
    print("Hello from unidad1-project!")
    file = Path("./data/netflix_titles.csv")
    reader = CSVReader(file)
    reader_generator = reader.read()
    #print(reader_generator)
    print([next(reader_generator) for _ in range(5)])




if __name__ == "__main__":
    main()
