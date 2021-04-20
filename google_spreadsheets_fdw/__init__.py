import logging
from datetime import datetime, date, time
from typing import Any, Dict, List, Generator

import gspread
from gspread import Cell
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from oauth2client.service_account import ServiceAccountCredentials

__all__ = ['GoogleSpreadsheetFDW']


def float_to_hms(f: float) -> (int, int, int):
    h, r = divmod(f, 1)
    m, r = divmod(r * 60, 1)
    return int(h), int(m), int(r * 60),


def pg_date_to_gs_date(val: date) -> float:
    delta = datetime.combine(val, time()) - datetime(1899, 12, 30)
    return float(delta.days) + (float(delta.seconds) / 86400)


def gs_date_to_pg_date(val: float) -> date:
    ordinal = datetime(1899, 12, 30).toordinal() + int(val)
    dt = datetime.fromordinal(ordinal)
    h, m, s = float_to_hms(val % 1)
    return dt.replace(hour=h, minute=m, second=s).date()


pg_to_gs_converters = {
    2950: lambda val: str(val) if val is not None else None,  # uuid
    1043: lambda val: str(val) if val is not None else None,  # varchar
    23: lambda val: int(val) if val is not None else None,  # int
    701: lambda val: float(val) if val is not None else None,  # float
    1082: lambda val: pg_date_to_gs_date(val) if val is not None else None  # date
}

gs_to_pg_converters = {
    2950: lambda val: str(val) if val != '' else None,  # uuid
    1043: lambda val: str(val) if val != '' else None,  # varchar
    23: lambda val: int(val) if val != '' else None,  # int
    701: lambda val: float(val) if val != '' else None,  # float
    1082: lambda val: gs_date_to_pg_date(val) if val != '' else None  # date
}


class GoogleSpreadsheetFDW(ForeignDataWrapper):

    @property
    def rowid_column(self):
        return self.row_id_column

    def __init__(self, options, columns):
        super(GoogleSpreadsheetFDW, self).__init__(options, columns)

        self.row_id_column = options.get("row_id")

        self.formula_columns = list(filter(
            None, options.get("formula_columns", "").split(",")
        ))

        self.value_input_option = options.get(
            "value_input_option", "USER_ENTERED"
        )

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

        self.columns_names = self.sheet.row_values(1)

    def execute(self, quals: List, columns: List) -> Generator:
        log(
            "EXECUTE %s %s" % (repr(quals), repr(columns)),
            logging.DEBUG
        )

        results = self.sheet.get_all_values('UNFORMATTED_VALUE')

        headers = results[0]

        results = map(
            lambda row: self.__convert_gs_row(row, headers), results[1:]
        )

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

        new_values_converted = self.__convert_pg_row(new_values)

        new_values_to_be_insert = [
            new_values_converted.get(c) for c in self.columns_names
        ]

        self.sheet.append_row(
            values=new_values_to_be_insert,
            value_input_option=self.value_input_option
        )

        return new_values

    def update(
            self, document_id: Any, new_values: Dict[str, Any]
    ) -> Dict[str, Any] or None:
        log(
            "UPDATE %s %s" % (repr(document_id), repr(new_values)),
            logging.DEBUG
        )

        new_values_converted = self.__convert_pg_row(new_values)

        row = self.__find_row_by_id(document_id)

        if row is None:
            return None

        cells = [
            Cell(
                row=row,
                col=self.__find_column_by_name(key),
                value=val if val is not None else ''
            )
            for (key, val) in new_values_converted.items()
            if key != self.rowid_column and key not in self.formula_columns
        ]

        self.sheet.update_cells(
            cell_list=cells,
            value_input_option=self.value_input_option
        )

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

    def __convert_value(
            self, name: str, value: Any, converters: Dict[int, callable]
    ) -> Any:
        column_definition = self.columns.get(name)

        if column_definition is None:
            return None

        converter = converters.get(
            column_definition.type_oid
        )

        if converter is None:
            raise Exception(
                "Unsupported data type %s (%s)" % (
                    column_definition.type_oid, column_definition.type_name
                )
            )

        try:
            return converter(value)
        except ValueError:
            log("Invalid value %s for column %s (%s)" % (
                value, name, column_definition.type_name
            ), logging.WARNING)
            return None

    def __convert_gs_row(self, row: List, headers) -> Dict[str, Any]:
        return dict({
            header: self.__convert_value(
                header, row[headers.index(header)], gs_to_pg_converters
            )
            for header in headers
        })

    def __convert_pg_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(map(
            lambda kv: (kv[0], self.__convert_value(
                kv[0], kv[1], pg_to_gs_converters
            )),
            row.items()
        ))
