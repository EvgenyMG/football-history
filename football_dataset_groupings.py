import pandas as pd

def group_by_country(df):
    #df = df.copy()
    # Grouping by 'country'
    grouped = df.groupby('country')

    # Creating a function to handle multiple custom aggregations
    def custom_aggregations(x):
        home_wins = (x['home_score'] > x['away_score']).sum()
        home_losses = (x['home_score'] < x['away_score']).sum()
        home_draws = (x['home_score'] == x['away_score']).sum()
        avg_goal_diff = (x['home_score'] - x['away_score']).mean()
        most_common_opponent = x['away_team'].mode()[0] if not x['away_team'].mode().empty else None
        most_common_city = x['city'].mode()[0] if not x['city'].mode().empty else None
        return pd.Series([home_wins, home_losses, home_draws, avg_goal_diff, most_common_opponent, most_common_city])

    # Applying custom aggregations along with standard ones
    grouped_df = grouped.agg(
        total_matches_hosted=pd.NamedAgg(column='id', aggfunc='count'),
        avg_home_score=pd.NamedAgg(column='home_score', aggfunc='mean'),
        avg_away_score=pd.NamedAgg(column='away_score', aggfunc='mean'),
        tournament_count=pd.NamedAgg(column='tournament', aggfunc=lambda x: x.nunique())
    ).join(grouped.apply(custom_aggregations))

    # Renaming columns for custom aggregations
    grouped_df.columns = [
        'Total Matches Hosted', 'Average Home Score', 'Average Away Score',
        'Tournament Count', 'Home Wins', 'Home Losses', 'Home Draws',
        'Average Goal Difference', 'Most Common Opponent', 'Most Common City'
    ]

    grouped_df.reset_index(inplace=True)

    return grouped_df



def group_by_tournament(df):
    # Grouping the data by 'tournament'
    grouped = df.groupby('tournament')

    # Calculating the required statistics
    grouped_df = pd.DataFrame({
        'matches_played': grouped['date'].count(),
        'unique_teams': grouped['home_team'].nunique(),
        'average_home_goals': grouped['home_score'].mean(),
        'average_away_goals': grouped['away_score'].mean(),
        'home_wins': grouped.apply(lambda x: (x['home_score'] > x['away_score']).sum()),
        'away_wins': grouped.apply(lambda x: (x['home_score'] < x['away_score']).sum()),
        'draws': grouped.apply(lambda x: (x['home_score'] == x['away_score']).sum()),
        # Splitting tournament date range into start and end dates
        'start_date': grouped['date'].min(),
        'end_date': grouped['date'].max(),
        # Adding most frequent location (country)
        'location': grouped['country'].agg(lambda x: x.value_counts().idxmax())
    })

    # Resetting index to make 'tournament' a column
    grouped_df.reset_index(inplace=True)

    # Determining tournament_winner
    home_wins_per_team = df[df['home_score'] > df['away_score']].groupby(['tournament', 'home_team']).size()
    away_wins_per_team = df[df['home_score'] < df['away_score']].groupby(['tournament', 'away_team']).size()
    total_wins_per_team = home_wins_per_team.add(away_wins_per_team, fill_value=0)
    tournament_winner = total_wins_per_team.groupby(level=0).idxmax().apply(lambda x: x[1])
    grouped_df['tournament_winner'] = grouped_df['tournament'].map(tournament_winner)

    return grouped_df





def group_by_year(df):
    # Convert 'date' to datetime just once
    df['year'] = pd.to_datetime(df['date']).dt.year

    # Initialize a groupby object once
    year_group = df.groupby('year')

    # 1. Match Counts by Year
    matches_per_year = year_group.size().rename('matches_count')

    # 2. Average Scores by Year
    average_scores_per_year = year_group[['home_score', 'away_score']].mean()

    # 3. Tournament Frequency per Year
    tournament_frequency_per_year = df.pivot_table(index='year', columns='tournament', aggfunc='size', fill_value=0)

    # 4. Team Performance per Year
    df['home_win'] = df['home_score'] > df['away_score']
    df['away_win'] = df['home_score'] < df['away_score']
    df['draw'] = df['home_score'] == df['away_score']

    # Combined team performance
    team_performance = df.melt(id_vars=['year', 'home_team', 'away_team'],
                               value_vars=['home_win', 'away_win', 'draw'],
                               var_name='result').groupby(['year', 'value']).sum()
    team_performance = team_performance.groupby(level='year').sum()  # Collapse the multiindex

    # 5. Geographical Analysis
    geo_analysis = year_group['country'].value_counts().unstack(fill_value=0)

    # Combining all these aggregations into a single dataframe
    grouped_df = pd.concat([matches_per_year, average_scores_per_year, tournament_frequency_per_year, team_performance, geo_analysis], axis=1)
    grouped_df.reset_index(inplace=True)

    return grouped_df





def group_by_team(df):
    #df = df.copy()
    # Function to determine the result of a match for both teams
    def determine_results(row):
        if row['home_score'] > row['away_score']:
            return 'win', 'loss'
        elif row['home_score'] < row['away_score']:
            return 'loss', 'win'
        else:
            return 'draw', 'draw'

    # Applying the function to the dataframe
    df[['home_result', 'away_result']] = df.apply(lambda row: determine_results(row), axis=1, result_type='expand')

    # Creating a DataFrame to hold the results for each team in each match
    matches = pd.concat([
        df[['home_team', 'home_result', 'tournament']].rename(columns={'home_team': 'team', 'home_result': 'result'}),
        df[['away_team', 'away_result', 'tournament']].rename(columns={'away_team': 'team', 'away_result': 'result'})
    ])

    # Aggregating results by team
    grouped_df = matches.groupby('team').agg({
        'result': ['count', lambda x: (x == 'win').sum(), lambda x: (x == 'loss').sum(), lambda x: (x == 'draw').sum()],
        'tournament': 'nunique'
    })

    # Renaming columns for clarity
    grouped_df.columns = ['matches_played', 'wins', 'losses', 'draws', 'tournaments']

    # Calculating home and away wins, losses, and matches played for each team
    home_wins = df[df['home_result'] == 'win'].groupby('home_team')['home_result'].count().rename('home_wins')
    away_wins = df[df['away_result'] == 'win'].groupby('away_team')['away_result'].count().rename('away_wins')
    home_losses = df[df['home_result'] == 'loss'].groupby('home_team')['home_result'].count().rename('home_losses')
    away_losses = df[df['away_result'] == 'loss'].groupby('away_team')['away_result'].count().rename('away_losses')
    home_matches_played = df.groupby('home_team')['home_team'].count().rename('home_matches_played')
    away_matches_played = df.groupby('away_team')['away_team'].count().rename('away_matches_played')

    # Merging these counts with the existing grouped_df DataFrame
    grouped_df = grouped_df.join([home_wins, away_wins, home_losses, away_losses, home_matches_played, away_matches_played], how='outer').fillna(0)

    grouped_df.reset_index(inplace=True)
    grouped_df.rename(columns={'index':'team'}, inplace=True)

    return grouped_df
