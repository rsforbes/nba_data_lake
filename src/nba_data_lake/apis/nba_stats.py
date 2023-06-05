import nba_api.stats.endpoints as nba
from nba_api.stats.library.parameters import (
    PerMode36,
    PlayerOrTeamAbbreviation,
    SeasonType,
)


class CommonAllPlayers:
    @staticmethod
    def get() -> str:
        return nba.CommonAllPlayers().get_json()


class PlayerGameLogs:
    @staticmethod
    def get(season: str, season_type: SeasonType = SeasonType.regular) -> str:
        return nba.LeagueGameLog(
            season=season,
            season_type_all_star=season_type,
            player_or_team_abbreviation=PlayerOrTeamAbbreviation.player,
            counter=30000,
        ).get_json()


class PlayerIndex:
    @staticmethod
    def get() -> str:
        return nba.PlayerIndex().get_json()


class PlayerProfile:
    @staticmethod
    def get(player_id: int) -> str:
        return nba.PlayerProfileV2(
            per_mode36=PerMode36.totals,
            player_id=player_id,
        ).get_json()
