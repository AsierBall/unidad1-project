from pathlib import Path
from unidad1_project import CSVReaderPandas, TransformerMissing, TransformerNormalizeStrings, WriterCsv, Orchestrator


# TODO: si nos da tiempo añadir cli con argparser o parse (opcion leer un fichero config con pydantic) ????
def main():

    file_path = Path("./data/netflix_titles.csv")
    file_path_wr = Path("./data/netflix_titles_wr.csv")

    reader = CSVReaderPandas(5)
    transformer_missings = TransformerMissing()
    transformer_normalize_strings = TransformerNormalizeStrings()
    writer = WriterCsv()

    orchestrator = Orchestrator(
        reader=reader,
        transformers=[
            transformer_missings,
            transformer_normalize_strings
            ],
        writer=writer
        )

    orchestrator.run(file_path, file_path_wr)


if __name__ == "__main__":
    main()
