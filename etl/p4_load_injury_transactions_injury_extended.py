import asyncio
import nba_data_lake.db.pro_sports_transactions.transactions_injuries_extended as db


async def main() -> None:
    # Create
    await db.TransactionsInjuriesExtended().create()

    # Load
    max_transactions = (
        await db.TransactionsInjuriesExtended().get_max_transaction_dates()
    )
    for max_transaction in max_transactions:
        max_transaction_date = max_transaction["max_transaction_date"]
        transaction_type = max_transaction["transaction_type"]
        print(
            f"Start Date: {max_transaction_date}, Transaction Type: {transaction_type}"
        )
        await db.TransactionsInjuriesExtended().load(
            start_date=max_transaction_date,
            transaction_type=transaction_type,
        )


if __name__ == "__main__":
    asyncio.run(main())
