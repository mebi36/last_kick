import pandas as pd


def process_data():
    data_file_path = "last_kick_data.xlsx"
    matches_df = pd.read_excel(data_file_path, sheet_name="matches")
    matches_df = (
        matches_df
        [[
            'id',
            'match_date',
            'home_team',
            'away_team',
            'home_team_ft_score',
            'away_team_ft_score'
        ]]
    )
    events_df = pd.read_excel(data_file_path, sheet_name="match_events")
    # print(events_df)
    matches_df["total_goals"] = (
        matches_df["home_team_ft_score"] + matches_df["away_team_ft_score"])
    matches_df["goal_times"] = (
        matches_df["id"]
        .apply(lambda x: [
            el
            for el in events_df[events_df["match_id"] == x]["event_time"].values
        ])
    )
    matches_df["first_goal_time"] = (
        matches_df["goal_times"].apply(lambda x: x[0] if len(x) > 0 else None))
    matches_df = (
        matches_df[
            ((matches_df["first_goal_time"] >= 75))
            | (matches_df["first_goal_time"].isna())
        ].reset_index(drop=True)
    )
    print("Total Bets: ", matches_df.shape[0])
    lost_bets = matches_df[matches_df["total_goals"] == 0].shape[0]
    won_bets = matches_df[matches_df["total_goals"] > 0].shape[0]
    stake_size = 10_000
    gross_loss = lost_bets*stake_size*-1
    gross_profit = won_bets*stake_size*1
    print("lost: ", lost_bets)
    print("won: ", won_bets)
    print(f"Gross loss: {gross_loss:,.2f}")
    print(f"Gross profit: {gross_profit:,.2f}")
    print(f"Net profit: {gross_profit+gross_loss:,.2f}")


if __name__ == "__main__":
    process_data()
