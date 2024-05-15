from datetime import datetime as dt

import pandas as pd
import pygsheets as pg


class Tools:
    @staticmethod
    def init_gc_client(credential_path: str):
        return pg.authorize(service_file=credential_path)

    @staticmethod
    def init_wks(gc, sheet_url: str, ws_name: str) -> pd.DataFrame:
        sh = gc.open_by_url(sheet_url)
        return sh.worksheet_by_title(ws_name)

    @staticmethod
    def update_wks(df: pd.DataFrame, wks: pg.Worksheet, cell: str, only_values: bool = True, first_clear: bool = False):
        if first_clear:
            wks.clear()
        if only_values:
            wks.update_values(cell, df.values.tolist())
        else:
            wks.set_dataframe(df, cell)
        return True

    @staticmethod
    def get_datetime() -> str:
        return dt.now().strftime("%Y-%m-%d %H:%M:%S")
