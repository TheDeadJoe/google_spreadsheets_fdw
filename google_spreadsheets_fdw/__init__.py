import logging
from typing import Any, Dict, List, Generator

import gspread
from gspread import Cell
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from oauth2client.service_account import ServiceAccountCredentials

__all__ = ['GoogleSpreadsheetFDW']


class GoogleSpreadsheetFDW(ForeignDataWrapper):

    @property
    def rowid_column(self):
        return self.row_id_column

    def __init__(self, options, columns):
        super(GoogleSpreadsheetFDW, self).__init__(options, columns)

        self.row_id_column = options.get("row_id")

        self.columns_names = [col.column_name for col in list(columns.values())]

        self.columns = columns

        scopes = [
            'https://spreadsheets.google.com/feeds'
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            options.get("keyfile"), scopes
        )

        client = gspread.authorize(credentials)

        self.sheet = client.open_by_key(
            options["gskey"]
        ).get_worksheet(
            int(options["sheet"])
        )

        self.converters = {
            2950: lambda val: str(val),  # uuid
            1043: lambda val: str(val),  # varchar
            23: lambda val: int(val),  # int
            701: lambda val: float(  # float
                str(val).replace(options.get("radixchar", "."), ".")
            ),
        }

    def execute(self, quals: List, columns: List) -> Generator:
        log(
            "EXECUTE %s %s" % (repr(quals), repr(columns)),
            logging.DEBUG
        )

        results = self.sheet.get_all_records()

        results = map(lambda row: self.__convert_row(row), results)

        for result in results:
            line = {}
            for column in columns:
                line[column] = result.get(column)
            yield line

    def insert(self, new_values: Dict[str, Any]) -> Dict[str, Any]:
        log(
            "INSERT %s" % (repr(new_values)),
            logging.DEBUG
        )

        new_values = self.__convert_row(new_values)

        new_values_to_be_insert = [new_values.get(c) for c in self.columns]

        self.sheet.append_row(new_values_to_be_insert)

        return new_values

    def update(
            self, document_id: Any, new_values: Dict[str, Any]
    ) -> Dict[str, Any] or None:
        log(
            "UPDATE %s %s" % (repr(document_id), repr(new_values)),
            logging.DEBUG
        )

        new_values = self.__convert_row(new_values)

        row = self.__find_row_by_id(document_id)

        if row is None:
            return None

        cells = [
            Cell(row=row, col=self.__find_column_by_name(key), value=val)
            for (key, val) in new_values.items()
            if key != self.rowid_column
        ]

        self.sheet.update_cells(cells)

        return new_values

    def delete(self, document_id: Any) -> None:
        log(
            "DELETE %s" % (repr(document_id)),
            logging.DEBUG
        )

        row = self.__find_row_by_id(document_id)

        if row is None:
            return None

        self.sheet.delete_row(row)

    def __find_column_by_name(self, name: str) -> int:
        column_idx = self.columns_names.index(name)

        if column_idx is None:
            raise Exception("Column %s not found" % name)

        return column_idx + 1

    def __find_row_by_id(self, query: str) -> int or None:
        row_id_column_idx = self.columns_names.index(self.rowid_column)

        if row_id_column_idx is None:
            return None

        cell = self.sheet.find(str(query), None, row_id_column_idx + 1)

        if cell is None:
            return None

        return cell.row

    def __convert_value(self, name: str, value: Any) -> Any:
        column_definition = self.columns.get(name)

        converter = self.converters.get(
            column_definition.type_oid
        )

        if converter is None:
            raise Exception(
                "Unsupported data type %s (%s)" % (
                    column_definition.type_oid, column_definition.type_name
                )
            )

        return converter(value)

    def __convert_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(map(
            lambda kv: (kv[0], self.__convert_value(*kv)),
            row.items()
        ))