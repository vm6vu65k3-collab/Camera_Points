import pandas as pd 
import numpy as np

def clean_region_name(df: pd.DataFrame) -> pd.DataFrame:
    city = df["city_name"].fillna("").str.strip().to_numpy(dtype = str)
    region = df['region_name'].fillna("").str.strip().to_numpy(dtype = str)
    
    startswith_mask = (region != "") & np.char.startswith(region, city)
    empty_mask = (region == "")

    cleaned = np.where(
        startswith_mask,
        [r[len(c):] for c, r in zip(city, region)],
        region
    )
    
    cleaned = np.where(
        empty_mask,
        city,
        cleaned
    )

    df["region_name"] = cleaned

    return df 

def df_to_records(df: pd.DataFrame) -> list[dict]:
    return df.where(pd.notna(df), None).to_dict(orient = "records")


