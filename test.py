import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

path_in = r"C:\COVIDPRO\data\processed\metadata_cleaned.csv"
path_out = r"C:\COVIDPRO\data\processed\metadata_cleaned_spark.parquet"

df = pd.read_csv(path_in, parse_dates=["date"])

table = pa.Table.from_pandas(df, preserve_index=False)

pq.write_table(
    table,
    path_out,
    coerce_timestamps="ms",             # приводим timestamp к миллисекундам
    allow_truncated_timestamps=True
)