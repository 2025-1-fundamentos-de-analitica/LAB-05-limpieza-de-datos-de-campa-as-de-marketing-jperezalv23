# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
from glob import glob

def clean_campaign_data():
    """
    Limpia los datos de una campaña de marketing contenida en archivos .zip dentro de files/input/.
    Se generan tres archivos CSV: client.csv, campaign.csv y economics.csv en files/output/.
    """
    input_path = "files/input/"
    output_path = "files/output/"
    os.makedirs(output_path, exist_ok=True)

    # Leer todos los archivos CSV dentro de los archivos ZIP
    dfs = []
    for zip_file in glob(os.path.join(input_path, "*.zip")):
        with zipfile.ZipFile(zip_file, 'r') as archive:
            for file_name in archive.namelist():
                if file_name.endswith(".csv"):
                    with archive.open(file_name) as file:
                        df = pd.read_csv(file, sep=",")
                        dfs.append(df)

    # Unir todos los DataFrames leídos
    full_data = pd.concat(dfs, ignore_index=True)

    # Asegurar que client_id está presente
    if "client_id" not in full_data.columns:
        full_data["client_id"] = range(1, len(full_data) + 1)

    # ================= CLIENT DATA ==================
    client_cols = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    client_df = full_data[client_cols].copy()

    client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False)
    client_df["education"] = client_df["education"].replace("unknown", pd.NA)

    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

    client_df.to_csv(os.path.join(output_path, "client.csv"), index=False)

    # ================= CAMPAIGN DATA ==================
    campaign_cols = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "day",
        "month"
    ]
    campaign_df = full_data[campaign_cols].copy()

    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if str(x).lower() == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

    # Crear la fecha en formato YYYY-MM-DD
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    campaign_df["month"] = campaign_df["month"].str.lower().map(month_map)
    campaign_df["day"] = campaign_df["day"].astype(int).astype(str).str.zfill(2)
    campaign_df["last_contact_date"] = "2022-" + campaign_df["month"] + "-" + campaign_df["day"]

    campaign_df = campaign_df[[
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date"
    ]]
    campaign_df.to_csv(os.path.join(output_path, "campaign.csv"), index=False)

    # ================= ECONOMICS DATA ==================
    econ_cols = ["client_id", "cons_price_idx", "euribor_three_months"]
    economics_df = full_data[econ_cols].copy()
    economics_df.to_csv(os.path.join(output_path, "economics.csv"), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()
