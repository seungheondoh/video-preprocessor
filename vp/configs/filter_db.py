import pandas as pd

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df['view_count'] >= 50000) \
        & (df['duration'] <= 3600)] # TODO(minhee): Adjust filtering condition later
    return df