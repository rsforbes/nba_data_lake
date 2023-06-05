import asyncio
import nba_data_lake.db.pro_sports_transactions.injury_notes_keyword_exclusions as db


async def main() -> None:
    await db.InjuryNotesKeywordExclusions().create()
    await db.InjuryNotesKeywordExclusions().load()


if __name__ == "__main__":
    asyncio.run(main())
