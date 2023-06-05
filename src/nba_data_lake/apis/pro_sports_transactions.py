from datetime import date
from pro_sports_transactions import League, Search, TransactionType
from typing import Dict


class ProSportsTransactions:
    _last_row = 0

    def __init__(
        self, start_date: date, end_date: date, transaction_type: TransactionType
    ) -> None:
        self._start_date = start_date
        self._end_date = end_date
        self._transaction_type = transaction_type

    async def get_next(self) -> Dict:
        search = Search(
            League.NBA,
            transaction_types=(self._transaction_type,),
            start_date=self._start_date,
            end_date=self._end_date,
            starting_row=self._last_row,
        )
        print(await search.get_url())
        results = await search.get_json()

        self._last_row += 25

        return results
