#!/usr/bin/env python3
"""
Analyze parking infractions and create visualizations
Generates charts showing patterns, trends, and insights
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

def create_visualizations():
    try:
        print('📡 Connecting to database...')

        with psycopg.connect(database_url) as conn:

            # 1. Top 15 Infraction Types by Count
            print('\n🎨 Creating visualization 1: Top 15 Infraction Types...')
            df1 = pd.read_sql("""
                SELECT
                    infraction_code,
                    infraction_description,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL AND infraction_description != ''
                GROUP BY infraction_code, infraction_description
                ORDER BY ticket_count DESC
                LIMIT 15
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 8))
            colors = plt.cm.RdYlGn_r(np.linspace(0.3, 0.7, len(df1)))
            bars = ax.barh(range(len(df1)), df1['ticket_count'], color=colors)
            ax.set_yticks(range(len(df1)))
            ax.set_yticklabels([f"[{code}] {desc[:50]}" for code, desc in zip(df1['infraction_code'], df1['infraction_description'])], fontsize=9)
            ax.set_xlabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Top 15 Most Common Parking Infractions', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, df1['ticket_count'])):
                ax.text(val, i, f' {val:,.0f}', va='center', fontweight='bold')

            plt.tight_layout()
            plt.savefig('01_top_15_infractions.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 01_top_15_infractions.png')
            plt.close()

            # 2. Average Fine by Infraction Type (Top 15)
            print('🎨 Creating visualization 2: Average Fine by Type...')
            df2 = pd.read_sql("""
                SELECT
                    infraction_code,
                    infraction_description,
                    COUNT(*) as ticket_count,
                    ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL AND infraction_description != ''
                  AND set_fine_amount IS NOT NULL AND set_fine_amount > 0
                GROUP BY infraction_code, infraction_description
                ORDER BY avg_fine DESC
                LIMIT 15
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 8))
            colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(df2)))
            bars = ax.barh(range(len(df2)), df2['avg_fine'], color=colors)
            ax.set_yticks(range(len(df2)))
            ax.set_yticklabels([f"[{code}] {desc[:50]}" for code, desc in zip(df2['infraction_code'], df2['infraction_description'])], fontsize=9)
            ax.set_xlabel('Average Fine ($)', fontsize=12, fontweight='bold')
            ax.set_title('Highest Average Fines by Infraction Type', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, df2['avg_fine'])):
                ax.text(val, i, f' ${val:.2f}', va='center', fontweight='bold')

            plt.tight_layout()
            plt.savefig('02_highest_average_fines.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 02_highest_average_fines.png')
            plt.close()

            # 3. Ticket Count Distribution by Year
            print('🎨 Creating visualization 3: Tickets by Year...')
            df3 = pd.read_sql("""
                SELECT
                    EXTRACT(YEAR FROM date_of_infraction)::INTEGER as year,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE date_of_infraction IS NOT NULL
                GROUP BY year
                ORDER BY year
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 7))
            ax.plot(df3['year'], df3['ticket_count'], marker='o', linewidth=3, markersize=8, color='#2E86AB')
            ax.fill_between(df3['year'], df3['ticket_count'], alpha=0.3, color='#2E86AB')
            ax.set_xlabel('Year', fontsize=12, fontweight='bold')
            ax.set_ylabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Parking Tickets Over Time (2008-2024)', fontsize=14, fontweight='bold', pad=20)
            ax.grid(True, alpha=0.3)

            # Add value labels
            for x, y in zip(df3['year'], df3['ticket_count']):
                ax.text(x, y, f'{int(y):,}', ha='center', va='bottom', fontsize=9, fontweight='bold')

            plt.tight_layout()
            plt.savefig('03_tickets_by_year.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 03_tickets_by_year.png')
            plt.close()

            # 4. Tickets by Month (aggregated across all years)
            print('🎨 Creating visualization 4: Tickets by Month...')
            df4 = pd.read_sql("""
                SELECT
                    EXTRACT(MONTH FROM date_of_infraction)::INTEGER as month,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE date_of_infraction IS NOT NULL
                GROUP BY month
                ORDER BY month
            """, conn)

            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

            fig, ax = plt.subplots(figsize=(14, 7))
            colors = plt.cm.coolwarm(np.linspace(0, 1, len(df4)))
            bars = ax.bar(range(len(df4)), df4['ticket_count'], color=colors, edgecolor='black', linewidth=1.5)
            ax.set_xticks(range(len(df4)))
            ax.set_xticklabels([month_names[int(m)-1] for m in df4['month']], fontsize=11, fontweight='bold')
            ax.set_ylabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Seasonal Parking Enforcement Patterns', fontsize=14, fontweight='bold', pad=20)

            # Add value labels
            for bar, val in zip(bars, df4['ticket_count']):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height, f'{int(val):,}',
                       ha='center', va='bottom', fontsize=9, fontweight='bold')

            plt.tight_layout()
            plt.savefig('04_tickets_by_month.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 04_tickets_by_month.png')
            plt.close()

            # 5. Tickets by Time of Day
            print('🎨 Creating visualization 5: Tickets by Hour of Day...')
            df5 = pd.read_sql("""
                SELECT
                    SUBSTRING(time_of_infraction, 1, 2)::INTEGER as hour,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE time_of_infraction IS NOT NULL AND time_of_infraction ~ '^[0-9]{4}$'
                GROUP BY hour
                ORDER BY hour
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 7))
            colors = plt.cm.twilight(np.linspace(0, 1, len(df5)))
            bars = ax.bar(df5['hour'], df5['ticket_count'], color=colors, edgecolor='black', linewidth=1.5, width=0.8)
            ax.set_xlabel('Hour of Day', fontsize=12, fontweight='bold')
            ax.set_ylabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Parking Enforcement Activity by Hour', fontsize=14, fontweight='bold', pad=20)
            ax.set_xticks(range(0, 24, 2))

            # Add value labels
            for bar, val in zip(bars, df5['ticket_count']):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height, f'{int(val):,}',
                       ha='center', va='bottom', fontsize=8, fontweight='bold')

            plt.tight_layout()
            plt.savefig('05_tickets_by_hour.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 05_tickets_by_hour.png')
            plt.close()

            # 6. Fine Amount Distribution
            print('🎨 Creating visualization 6: Fine Amount Distribution...')
            df6 = pd.read_sql("""
                WITH binned_fines AS (
                    SELECT
                        CASE
                            WHEN set_fine_amount::numeric <= 30 THEN 1
                            WHEN set_fine_amount::numeric <= 50 THEN 2
                            WHEN set_fine_amount::numeric <= 75 THEN 3
                            WHEN set_fine_amount::numeric <= 100 THEN 4
                            ELSE 5
                        END as bin_order,
                        CASE
                            WHEN set_fine_amount::numeric <= 30 THEN '$0-30'
                            WHEN set_fine_amount::numeric <= 50 THEN '$31-50'
                            WHEN set_fine_amount::numeric <= 75 THEN '$51-75'
                            WHEN set_fine_amount::numeric <= 100 THEN '$76-100'
                            ELSE '$100+'
                        END as fine_range
                    FROM parking_tickets
                    WHERE set_fine_amount IS NOT NULL AND set_fine_amount > 0
                )
                SELECT fine_range, COUNT(*) as ticket_count
                FROM binned_fines
                GROUP BY bin_order, fine_range
                ORDER BY bin_order
            """, conn)

            fig, ax = plt.subplots(figsize=(12, 8))
            colors = ['#2ecc71', '#f39c12', '#e74c3c', '#c0392b', '#7f1b32']
            wedges, texts, autotexts = ax.pie(df6['ticket_count'], labels=df6['fine_range'],
                                               autopct='%1.1f%%', colors=colors, startangle=90,
                                               textprops={'fontsize': 12, 'fontweight': 'bold'})
            ax.set_title('Distribution of Fines by Amount', fontsize=14, fontweight='bold', pad=20)

            # Make percentage text more readable
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(11)

            plt.tight_layout()
            plt.savefig('06_fine_distribution.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 06_fine_distribution.png')
            plt.close()

            # 7. Top 10 Streets with Most Infractions
            print('🎨 Creating visualization 7: Top 10 Streets...')
            df7 = pd.read_sql("""
                SELECT
                    street_normalized,
                    COUNT(*) as ticket_count
                FROM parking_tickets
                WHERE street_normalized IS NOT NULL
                  AND street_normalized != ''
                  AND street_normalized NOT LIKE '%$%'
                  AND street_normalized NOT LIKE '%*%'
                GROUP BY street_normalized
                ORDER BY ticket_count DESC
                LIMIT 10
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 8))
            colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(df7)))
            bars = ax.barh(range(len(df7)), df7['ticket_count'], color=colors, edgecolor='black', linewidth=1.5)
            ax.set_yticks(range(len(df7)))
            ax.set_yticklabels(df7['street_normalized'], fontsize=11, fontweight='bold')
            ax.set_xlabel('Number of Tickets', fontsize=12, fontweight='bold')
            ax.set_title('Top 10 Streets with Most Parking Infractions', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, df7['ticket_count'])):
                ax.text(val, i, f' {val:,.0f}', va='center', fontweight='bold')

            plt.tight_layout()
            plt.savefig('07_top_streets.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 07_top_streets.png')
            plt.close()

            # 8. Revenue by Infraction Type (Top 10)
            print('🎨 Creating visualization 8: Revenue by Infraction...')
            df8 = pd.read_sql("""
                SELECT
                    infraction_code,
                    infraction_description,
                    COUNT(*) as ticket_count,
                    SUM(set_fine_amount::numeric) as total_revenue
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL
                  AND infraction_description != ''
                  AND set_fine_amount IS NOT NULL
                GROUP BY infraction_code, infraction_description
                ORDER BY total_revenue DESC
                LIMIT 10
            """, conn)

            fig, ax = plt.subplots(figsize=(14, 8))
            colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(df8)))
            bars = ax.barh(range(len(df8)), df8['total_revenue']/1_000_000, color=colors, edgecolor='black', linewidth=1.5)
            ax.set_yticks(range(len(df8)))
            ax.set_yticklabels([f"[{code}] {desc[:50]}" for code, desc in zip(df8['infraction_code'], df8['infraction_description'])], fontsize=9)
            ax.set_xlabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
            ax.set_title('Top 10 Infraction Types by Total Revenue Generated', fontsize=14, fontweight='bold', pad=20)
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, df8['total_revenue']/1_000_000)):
                ax.text(val, i, f' ${val:.1f}M', va='center', fontweight='bold')

            plt.tight_layout()
            plt.savefig('08_revenue_by_infraction.png', dpi=300, bbox_inches='tight')
            print('   ✅ Saved: 08_revenue_by_infraction.png')
            plt.close()

            # Print summary
            print('\n' + '='*60)
            print('📊 ANALYSIS SUMMARY')
            print('='*60)

            summary = pd.read_sql("""
                SELECT
                    COUNT(*) as total_tickets,
                    COUNT(DISTINCT infraction_code) as unique_codes,
                    COUNT(DISTINCT street_normalized) as unique_streets,
                    COUNT(DISTINCT DATE(date_of_infraction)) as enforcement_days,
                    ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine,
                    SUM(set_fine_amount::numeric) as total_revenue,
                    MIN(date_of_infraction) as first_ticket,
                    MAX(date_of_infraction) as last_ticket
                FROM parking_tickets
                WHERE infraction_description IS NOT NULL AND set_fine_amount > 0
            """, conn).iloc[0]

            print(f'\n📈 TICKETS:')
            print(f'   Total tickets: {summary["total_tickets"]:,}')
            print(f'   Enforcement days: {summary["enforcement_days"]:,}')
            print(f'   Average tickets per day: {summary["total_tickets"]/summary["enforcement_days"]:,.0f}')

            print(f'\n🏷️  INFRACTIONS:')
            print(f'   Unique infraction codes: {summary["unique_codes"]}')
            print(f'   Unique streets: {summary["unique_streets"]:,}')

            print(f'\n💵 FINES:')
            print(f'   Average fine: ${summary["avg_fine"]:.2f}')
            print(f'   Total revenue: ${summary["total_revenue"]:,.2f}')
            print(f'   Total revenue (millions): ${summary["total_revenue"]/1_000_000:.1f}M')

            print(f'\n📅 TIME PERIOD:')
            print(f'   First ticket: {summary["first_ticket"]}')
            print(f'   Last ticket: {summary["last_ticket"]}')
            print(f'   Years of data: {summary["last_ticket"].year - summary["first_ticket"].year + 1}')

            print('\n' + '='*60)
            print('✅ All visualizations created successfully!')
            print('='*60 + '\n')

    except Exception as err:
        print(f'❌ Error: {err}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    create_visualizations()
