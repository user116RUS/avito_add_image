import logging
from datetime import datetime
from traceback import format_exc
from typing import List

import httplib2
from django.conf import settings
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

from bot.exceptions import GoogleSheetsError


class GoogleSheetsAPI:
    def __init__(self, spreadsheet_id: dict):
        self._CREDENTIALS_FILE = settings.BASE_DIR / "bot" / "static" / "google_cred.json"

        self.kzn_spreadsheet_id = spreadsheet_id["kzn"]
        self.nch_spreadsheet_id = spreadsheet_id["nch"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self._CREDENTIALS_FILE,
            ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        http_auth = credentials.authorize(httplib2.Http())

        self._service = discovery.build("sheets", "v4", http=http_auth)

        self.CITY_TO_SPREADSHEETS_ID = {"кзн": self.kzn_spreadsheet_id, "нч": self.nch_spreadsheet_id}

        self.kzn_schools_list = ["113", "13", "113+13", "126", "15", "167", "180", "80", "J-GET", "18"]

    @property
    def sheet(self) -> str:
        return str(datetime.now().month)

    def append_row(self, data: List[str]) -> None:
        """
        Appends row to receipts google sheets file
        Args:
            data: List[str] - row to append
        """
        try:
            self._service.spreadsheets().values().append(
                spreadsheetId=(
                    self.kzn_spreadsheet_id if data[0] in self.kzn_schools_list else self.nch_spreadsheet_id
                ),
                range=f"{self.sheet}!A:G",
                valueInputOption="USER_ENTERED",
                body={"majorDimension": "ROWS", "values": [data]},
            ).execute()
        except Exception as e:
            logging.error(f"Cannot append row to Google sheets. {e} {format_exc()}")
            raise GoogleSheetsError

    def get_or_create_sheet(self, sheet_name: str):
        """Получает лист по имени, если его нет - создает."""
        sheets = self._service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        sheet = next((s for s in sheets["sheets"] if s["properties"]["title"] == sheet_name), None)

        if not sheet:
            sheet = (
                self._service.spreadsheets()
                .addSheet(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
                )
                .execute()
            )
        return sheet["sheetId"]
