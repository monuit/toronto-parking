#!/usr/bin/env python3
"""
Analyze infraction descriptions + streets together
Shows which violations happen on which streets with visualizations
"""

import os
import psycopg
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import numpy as np

load_dotenv('map-app/.env.local')

# Try multiple env var names
database_url = os.getenv('DATABASE_URL') or os.getenv('TILES_DB_URL') or os.getenv('CORE_DB_URL')
if not database_url:
    print('❌ DATABASE_URL, TILES_DB_URL, or CORE_DB_URL not found')
    exit(1)

# Set style for better-looking charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

def analyze_description_street_pairs():
    try:
        print('📡 Connecting to database...')

        with psycopg.connect(database_url) as conn:

            # 1. Export all distinct infraction description + street pairs
            print('\n💾 Exporting description + street pairs to CSV...')
            df_pairs = pd.read_sql("""
                SELECT DISTINCT
                    infraction_description,
                    street_normalized,
                    COUNT(*) as ticket_count,
                    ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine,
                    MIN(date_of_infraction) as first_ticket_date,
                    MAX(date_of_infraction) as last_ticket_date
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL
                  AND infraction_description != ''
                  AND street_normalized IS NOT NULL
                  AND street_normalized != ''
                  AND street_normalized NOT LIKE '%$%'
                  AND street_normalized NOT LIKE '%*%'
                GROUP BY infraction_description, street_normalized
                ORDER BY ticket_count DESC
            """, conn)

            csv_file = 'infraction_description_street_pairs.csv'
            df_pairs.to_csv(csv_file, index=False)
            print(f'   ✅ {csv_file} created ({len(df_pairs):,} rows)')

            # 2. Top 20 description-street combinations
            print('\n🎨 Creating visualization 1: Top 20 Description-Street Pairs...')
            df_top_pairs = df_pairs.head(20).copy()
            df_top_pairs['label'] = df_top_pairs['infraction_description'].str[:30] + ' @ ' + df_top_pairs['street_normalized'].str[:20]

            fig, ax = plt.subplots(figsize=(14, 10))
            colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(df_top_pairs)))
            bars = ax.barh(range(len(df_top_pairs)), df_top_pairs['ticket_count'], color=colors, edgecolor='black', linewidth=1.5)
            ax.set_yticks(range(len(df_top_pairs)))
            ax.set_yticklabels(df_top_pairs['label'], fontsize=9)
            ax.set_xlabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Top 20 Infraction Description-Street Combinations', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, df_top_pairs['ticket_count'])):
                ax.text(val, i, f' {val:,}', va='center', fontweight='bold', fontsize=9)

            plt.tight_layout()
            plt.savefig('09_top_description_street_pairs.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 09_top_description_street_pairs.png')
            plt.close()

            # 3. Heatmap of top descriptions x top streets
            print('\n🎨 Creating visualization 2: Description-Street Heatmap...')

            # Get top 15 descriptions
            df_top_desc = pd.read_sql("""
                SELECT DISTINCT
                    infraction_description,
                    COUNT(*) as count
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL AND infraction_description != ''
                GROUP BY infraction_description
                ORDER BY count DESC
                LIMIT 15
            """, conn)

            # Get top 15 streets
            df_top_streets = pd.read_sql("""
                SELECT DISTINCT
                    street_normalized,
                    COUNT(*) as count
                FROM parking_tickets
                WHERE street_normalized IS NOT NULL
                  AND street_normalized != ''
                  AND street_normalized NOT LIKE '%$%'
                  AND street_normalized NOT LIKE '%*%'
                GROUP BY street_normalized
                ORDER BY count DESC
                LIMIT 15
            """, conn)

            # Create pivot table
            df_heatmap = pd.read_sql(f"""
                SELECT
                    infraction_description,
                    street_normalized,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE infraction_description IN ({','.join([repr(d) for d in df_top_desc['infraction_description']])})
                  AND street_normalized IN ({','.join([repr(s) for s in df_top_streets['street_normalized']])})
                GROUP BY infraction_description, street_normalized
            """, conn)

            pivot = df_heatmap.pivot_table(
                index='infraction_description',
                columns='street_normalized',
                values='ticket_count',
                fill_value=0
            )

            fig, ax = plt.subplots(figsize=(16, 10))
            sns.heatmap(pivot, cmap='YlOrRd', ax=ax, cbar_kws={'label': 'Ticket Count'}, fmt='g')
            ax.set_title('Heatmap: Top Infraction Descriptions vs Top Streets', fontsize=14, fontweight='bold', pad=20)
            ax.set_xlabel('Street', fontsize=12, fontweight='bold')
            ax.set_ylabel('Infraction Description', fontsize=12, fontweight='bold')
            plt.xticks(rotation=45, ha='right', fontsize=9)
            plt.yticks(fontsize=9)
            plt.tight_layout()
            plt.savefig('10_description_street_heatmap.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 10_description_street_heatmap.png')
            plt.close()

            # 4. For each street, show top 5 violations
            print('\n🎨 Creating visualization 3: Top Violations by Street...')
            df_street_desc = pd.read_sql("""
                SELECT
                    street_normalized,
                    infraction_description,
                    COUNT(*) as ticket_count,
                    ROW_NUMBER() OVER (PARTITION BY street_normalized ORDER BY COUNT(*) DESC) as rank
                FROM parking_tickets
                WHERE street_normalized IS NOT NULL
                  AND street_normalized != ''
                  AND street_normalized NOT LIKE '%$%'
                  AND street_normalized NOT LIKE '%*%'
                  AND infraction_description IS NOT NULL
                GROUP BY street_normalized, infraction_description
            """, conn)

            # Get top violations for top 10 streets
            df_top10_streets = df_street_desc.groupby('street_normalized')['ticket_count'].sum().nlargest(10).index.tolist()
            df_street_top5 = df_street_desc[
                (df_street_desc['street_normalized'].isin(df_top10_streets)) &
                (df_street_desc['rank'] <= 5)
            ].sort_values('street_normalized')

            fig, ax = plt.subplots(figsize=(14, 10))

            streets = df_street_top5['street_normalized'].unique()
            x_pos = 0
            colors_cycle = plt.cm.Set3(np.linspace(0, 1, 5))

            for street in streets:
                df_street = df_street_top5[df_street_top5['street_normalized'] == street]
                for rank, (idx, row) in enumerate(df_street.iterrows()):
                    ax.barh(x_pos, row['ticket_count'], color=colors_cycle[rank],
                           label=f'Rank {row["rank"]}' if street == streets[0] else '', height=0.8)
                    x_pos += 1

            ax.set_yticks(range(len(df_street_top5)))
            labels = [f"{row['street_normalized'][:15]} - {row['infraction_description'][:30]}"
                     for _, row in df_street_top5.iterrows()]
            ax.set_yticklabels(labels, fontsize=8)
            ax.set_xlabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Top 5 Violations for Each of Top 10 Streets', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            plt.tight_layout()
            plt.savefig('11_top_violations_per_street.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 11_top_violations_per_street.png')
            plt.close()

            # 5. Distribution of violations per street (how many unique violations per street)
            print('\n🎨 Creating visualization 4: Violation Diversity by Street...')
            df_diversity = pd.read_sql("""
                SELECT
                    street_normalized,
                    COUNT(DISTINCT infraction_description) as unique_violations,
                    COUNT(*) as total_tickets,
                    ROUND(COUNT(*)::numeric / COUNT(DISTINCT infraction_description), 2) as avg_tickets_per_violation
                FROM parking_tickets
                WHERE street_normalized IS NOT NULL
                  AND street_normalized != ''
                  AND street_normalized NOT LIKE '%$%'
                  AND street_normalized NOT LIKE '%*%'
                  AND infraction_description IS NOT NULL
                GROUP BY street_normalized
                ORDER BY unique_violations DESC
                LIMIT 20
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 8))

            scatter = ax.scatter(df_diversity['unique_violations'],
                               df_diversity['total_tickets'],
                               s=df_diversity['total_tickets']/10,
                               c=df_diversity['avg_tickets_per_violation'],
                               cmap='plasma', alpha=0.6, edgecolors='black', linewidth=1.5)

            for idx, row in df_diversity.iterrows():
                ax.annotate(row['street_normalized'][:15],
                          (row['unique_violations'], row['total_tickets']),
                          fontsize=8, alpha=0.7)

            ax.set_xlabel('Number of Unique Violations', fontsize=12, fontweight='bold')
            ax.set_ylabel('Total Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Violation Diversity by Street (Top 20)', fontsize=14, fontweight='bold', pad=20)
            cbar = plt.colorbar(scatter, ax=ax)
            cbar.set_label('Avg Tickets per Violation', fontsize=11, fontweight='bold')

            plt.tight_layout()
            plt.savefig('12_violation_diversity_by_street.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 12_violation_diversity_by_street.png')
            plt.close()

            # Print summary
            print('\n' + '='*70)
            print('📊 INFRACTION DESCRIPTION + STREET ANALYSIS SUMMARY')
            print('='*70)

            summary = pd.read_sql("""
                SELECT
                    COUNT(DISTINCT infraction_description || '|' || street_normalized) as unique_pairs,
                    COUNT(DISTINCT infraction_description) as unique_descriptions,
                    COUNT(DISTINCT street_normalized) as unique_streets,
                    COUNT(*) as total_tickets,
                    ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine,
                    MAX(ticket_count) as max_tickets_for_pair,
                    MIN(ticket_count) as min_tickets_for_pair
                FROM (
                    SELECT
                        infraction_description,
                        street_normalized,
                        COUNT(*) as ticket_count,
                        set_fine_amount
                    FROM parking_tickets
                    WHERE street_normalized IS NOT NULL
                      AND street_normalized != ''
                      AND infraction_description IS NOT NULL
                      AND infraction_description != ''
                    GROUP BY infraction_description, street_normalized, set_fine_amount
                ) subq
            """, conn).iloc[0]

            print(f'\n🎯 UNIQUE COMBINATIONS:')
            print(f'   Description-Street pairs: {summary["unique_pairs"]:,}')
            print(f'   Unique descriptions: {summary["unique_descriptions"]}')
            print(f'   Unique streets: {summary["unique_streets"]:,}')

            print(f'\n📊 DISTRIBUTION:')
            print(f'   Total tickets analyzed: {summary["total_tickets"]:,}')
            print(f'   Average fine per pair: ${summary["avg_fine"]}')
            print(f'   Max tickets for single pair: {summary["max_tickets_for_pair"]:,}')
            print(f'   Min tickets for single pair: {summary["min_tickets_for_pair"]}')

            # Show top 5 pairs
            print(f'\n🏆 TOP 5 DESCRIPTION-STREET PAIRS:\n')
            for idx, row in df_pairs.head(5).iterrows():
                print(f'{idx+1}. {row["infraction_description"]} @ {row["street_normalized"]}')
                print(f'   Tickets: {row["ticket_count"]:,} | Avg Fine: ${row["avg_fine"]}')
                print(f'   Date range: {row["first_ticket_date"]} to {row["last_ticket_date"]}\n')

            print('='*70)
            print('✅ Analysis complete!')
            print('='*70 + '\n')

    except Exception as err:
        print(f'❌ Error: {err}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    analyze_description_street_pairs()
