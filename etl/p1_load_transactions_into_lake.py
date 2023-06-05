from datetime import date
from pro_sports_transactions import TransactionType
from typing import Dict
import asyncio
import json
import nba_data_lake.apis.pro_sports_transactions as api
import nba_data_lake.lake.pro_sports_transactions.transactions as lake


async def load(start_date, end_date, transaction_type) -> Dict:
    transactions = []
    search = api.ProSportsTransactions(
        start_date=start_date, end_date=end_date, transaction_type=transaction_type
    )
    count = 1
    while True:
        print(
            f"Transaction Type: {transaction_type.name}, Start Date: {start_date}, End Date: {end_date}, Batch: {count}"
        )
        response = json.loads(await search.get_next())
        if len(response["transactions"]) == 0:
            break
        else:
            await lake.Transactions().insert(
                transactions=response["transactions"], transaction_type=transaction_type
            )
        count += 1

    return transactions


async def main() -> None:
    # Create
    await lake.Transactions().create_table()

    # start_date = date.fromisoformat("1800-01-01")
    max_transactions = await lake.Transactions().get_max_transaction_dates()
    end_date = date.today()
    for max_transaction in max_transactions:
        # Retrieve
        await load(
            date.fromisoformat(max_transaction["max_transaction_date"]),
            end_date,
            TransactionType[max_transaction["transaction_type"]],
        )


if __name__ == "__main__":
    asyncio.run(main())
