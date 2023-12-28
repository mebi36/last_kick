use polars::prelude::*;


fn example(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?
        .has_header(true)
        .finish()
}

fn main() {
    let matches_df = example("matches.csv").unwrap();
    let events_df = example("match_events.csv").unwrap();
    let event_df_for_join = events_df
        .clone()
        .lazy()
        .select([col("match_id"), col("event_time")])
        .collect()
        .unwrap();

    let matches_df = matches_df
        .clone()
        .lazy()
        .select([
            col("id"),
            col("match_date"),
            col("home_team"),
            col("away_team"),
            col("home_team_ft_score"),
            col("away_team_ft_score"),
            (col("home_team_ft_score") + col("away_team_ft_score")).alias("total_goals"),
        ])
        .collect()
        .unwrap();
    let comb_df = matches_df.left_join(&event_df_for_join, ["id"], ["match_id"]).unwrap();
    let comb_df = comb_df
            .clone()
            .lazy()
            .select([
                col("*"),
                col("event_time").min().over(["id"]).alias("first_goal_time")
            ])
            .unique(Some(vec!["id".to_string()]), UniqueKeepStrategy::First)
            .filter((col("first_goal_time").gt_eq(75)).or(col("first_goal_time").is_null()))
            .collect()
            .unwrap();

    let total_bets = comb_df.shape().0;
    let total_bets: i64 = total_bets as i64;
    let lost_bets = comb_df
        .clone()
        .lazy()
        .filter(col("total_goals").eq(0))
        .collect()
        .unwrap()
        .shape().0;
    let lost_bets = lost_bets as i64;
    let won_bets: i64 = total_bets - lost_bets;
    let stake_size: i64 = 10_000;
    let gross_loss = lost_bets * stake_size * -1;
    let gross_profit = won_bets * stake_size * 1;
    let net_profit = gross_loss + gross_profit;
    println!("Total bets: {total_bets}");
    println!("Won bets: {won_bets}");
    println!("Lost bets: {lost_bets}");
    println!("Gross loss: {gross_loss}");
    println!("Gross profit: {gross_profit}");
    println!("Net profit: {net_profit}");
}