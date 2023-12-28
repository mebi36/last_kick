import polars as pl


def process_data():
    matches_df = pl.read_csv("rs_version/matches.csv")
    events_df = pl.read_csv("rs_version/match_events.csv")

    events_df_for_join = events_df.select("match_id", "event_time")
    matches_df = (
        matches_df
        .select(
            "id",
            "match_date",
            "home_team",
            "away_team",
            "home_team_ft_score",
            "away_team_ft_score",
        )
        .with_columns(
            (
                pl.col("home_team_ft_score")
                + pl.col("away_team_ft_score")
            ).alias("total_goals")
        )
        .join(
            events_df_for_join, left_on="id", right_on="match_id", how="left")
        .with_columns(
            pl.col("event_time").min().over("id").alias("first_goal_time"))
        .unique("id", keep="first")
        .filter(
            (pl.col("first_goal_time") >= 75)
            | (pl.col("first_goal_time").is_null())
        )
    )
    print("Total bets: ", matches_df.shape[0])
    lost_bets = matches_df.filter(pl.col("total_goals") == 0).shape[0]
    won_bets = matches_df.shape[0] - lost_bets
    stake_size = 10_000
    gross_loss = lost_bets * stake_size * -1
    gross_profit = won_bets * stake_size * 1
    print("Lost bets: ", lost_bets)
    print("Won bets: ", won_bets)
    print(f"Gross loss: {gross_loss:,.2f}")
    print(f"Gross profit: {gross_profit:,.2f}")
    print(f"Net profit: {gross_loss+gross_profit:,.2f}")


if __name__ == "__main__":
    process_data()
