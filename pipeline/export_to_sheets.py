import re
import duckdb
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

DUCKDB_PATH = "/Users/mahatmaditya.syarief/Documents/personal_portfolios/miracle_solework/data/warehouse/miracle_solework.duckdb"
JSON_KEY_PATH = "../json/miracle-solework-pipeline-99fa1961657f.json"
SCHEMA = "main"

MARTS = [
    "fct_daily_finance",
    "fct_orders",
    "fct_expenses",
    "mart_customer_summary",
    "mart_customer_summary_active",
    "mart_customer_summary_sim",
    "mart_product_performance",
]


def get_sheet_id(url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        raise ValueError("URL Google Sheets is invalid.")
    return match.group(1)


def build_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(JSON_KEY_PATH, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def get_existing_tabs(service, spreadsheet_id: str) -> set:
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return {sheet["properties"]["title"] for sheet in metadata.get("sheets", [])}


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str, existing_tabs: set) -> set:
    if tab_name in existing_tabs:
        return existing_tabs

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": tab_name
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    existing_tabs.add(tab_name)
    return existing_tabs


def table_to_values(con, schema: str, table_name: str) -> list:
    df = con.execute(f"SELECT * FROM {schema}.{table_name}").df()
    df = df.where(df.notna(), "")
    return [df.columns.tolist()] + df.astype(str).values.tolist()


def clear_tab(service, spreadsheet_id: str, tab_name: str) -> None:
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'"
    ).execute()


def write_tab(service, spreadsheet_id: str, tab_name: str, values: list) -> None:
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def main():
    sheet_url = input("Paste Google Sheets URL: ").strip()
    spreadsheet_id = get_sheet_id(sheet_url)

    print("Connecting to Google Sheets API...")
    service = build_service()

    print("Connecting to DuckDB...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    existing_tabs = get_existing_tabs(service, spreadsheet_id)

    for mart in MARTS:
        print(f"Exporting {mart}...")
        try:
            values = table_to_values(con, SCHEMA, mart)
            existing_tabs = ensure_tab_exists(service, spreadsheet_id, mart, existing_tabs)
            clear_tab(service, spreadsheet_id, mart)
            write_tab(service, spreadsheet_id, mart, values)
            print(f"  done: {len(values) - 1} rows written")
        except Exception as e:
            print(f"  error on {mart}: {e}")

    con.close()
    print("Finished exporting marts to Google Sheets.")


if __name__ == "__main__":
    main()