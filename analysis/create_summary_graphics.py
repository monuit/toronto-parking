#!/usr/bin/env python3
"""
Create a final summary graphic explaining the discrepancy.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np

fig, ax = plt.subplots(figsize=(14, 10))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
title = ax.text(5, 9.5, 'School-Adjacent ASE Cameras: Data vs. Reality',
                fontsize=20, fontweight='bold', ha='center')

# Left side: What you observed
y_pos = 8.5
ax.add_patch(FancyBboxPatch((0.2, y_pos-1.2), 4.5, 1.2,
                            boxstyle="round,pad=0.1",
                            edgecolor='#2ecc71', facecolor='#d5f4e6', linewidth=2.5))
ax.text(2.45, y_pos-0.3, '👁️ YOUR FIELD OBSERVATIONS', fontsize=13, fontweight='bold', ha='center')
ax.text(2.45, y_pos-0.7, '7+ cameras near school entrances\n(5 in Ward 5 + 2 elsewhere)',
        fontsize=10, ha='center', va='top')

# Right side: What data showed
ax.add_patch(FancyBboxPatch((5.3, y_pos-1.2), 4.5, 1.2,
                            boxstyle="round,pad=0.1",
                            edgecolor='#3498db', facecolor='#d6eaf8', linewidth=2.5))
ax.text(7.55, y_pos-0.3, '📊 WHAT DATABASE SHOWED', fontsize=13, fontweight='bold', ha='center')
ax.text(7.55, y_pos-0.7, '179 cameras within 150m of\n585 schools (30.2% of network)',
        fontsize=10, ha='center', va='top')

# Arrow and question
ax.annotate('', xy=(5.3, 7.7), xytext=(4.7, 7.7),
            arrowprops=dict(arrowstyle='<->', color='red', lw=2))
ax.text(5, 7.9, 'DISCREPANCY?', fontsize=11, fontweight='bold', ha='center', color='red')

# Distance bands breakdown
y_pos = 6.8
ax.text(0.5, y_pos, 'Distance Breakdown of 179 Cameras:', fontsize=11, fontweight='bold')

bands = [
    ('0-25m\n(VERY CLOSE)', 3, '#27ae60', 'You DEFINITELY see these'),
    ('25-50m\n(CLOSE)', 16, '#f39c12', 'You would recognize these'),
    ('50-100m\n(MODERATE)', 83, '#e67e22', 'Depends on school layout'),
    ('100-150m\n(EDGE)', 81, '#e74c3c', 'Could be 2 blocks away')
]

x_offset = 0.3
for label, count, color, note in bands:
    # Draw box
    box_height = count / 20  # Scale for visibility
    ax.add_patch(FancyBboxPatch((x_offset, 3), 2, box_height,
                                edgecolor=color, facecolor=color, alpha=0.3, linewidth=2))
    # Label
    ax.text(x_offset + 1, 3 + box_height + 0.2, label, fontsize=9, ha='center', fontweight='bold')
    ax.text(x_offset + 1, 3 - 0.3, f'{count}\ncameras', fontsize=8, ha='center')
    ax.text(x_offset + 1, 2.3, note, fontsize=7, ha='center', style='italic', color='gray')
    x_offset += 2.4

# Key finding box
y_pos = 1.8
ax.add_patch(FancyBboxPatch((0.2, y_pos-1.3), 9.6, 1.3,
                            boxstyle="round,pad=0.1",
                            edgecolor='#c0392b', facecolor='#fadbd8', linewidth=2.5))
ax.text(5, y_pos-0.2, '🔍 KEY FINDING: DATA ISSUE, NOT METHODOLOGY ISSUE',
        fontsize=12, fontweight='bold', ha='center', color='#c0392b')
ax.text(5, y_pos-0.6, 'Only 3 cameras show as truly "at entrance" (0-25m). Your observations suggest either:\n' +
        '1️⃣ School dataset is incomplete (missing schools in your area)\n' +
        '2️⃣ School coordinates are offset from actual entrances (points to building, not entrance)',
        fontsize=9, ha='center', va='top')

plt.tight_layout()
plt.savefig('visualizations/05_discrepancy_summary.png', dpi=300, bbox_inches='tight', facecolor='white')
print("✓ Saved: 05_discrepancy_summary.png")
plt.close()

# Create a second graphic: What this means
fig, ax = plt.subplots(figsize=(14, 8))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

title = ax.text(5, 9.5, 'What the Discrepancy Means for Policy Analysis',
                fontsize=18, fontweight='bold', ha='center')

# Three scenarios
scenarios = [
    {
        'title': 'Scenario 1: School Data is Old',
        'icon': '📅',
        'description': 'CSV imported 2-3 years ago\nNew schools built since then\nSome cameras added after data collection',
        'implication': 'TRUE school-zone cameras > 179\nYour observations validate this',
        'color': '#f39c12',
        'x': 1.5
    },
    {
        'title': 'Scenario 2: Coordinates are Off',
        'icon': '📍',
        'description': 'Points to school center/gym\nNot to actual entrance\nMany schools have multiple entries',
        'implication': 'Truly adjacent cameras may appear\n50-100m away in our data',
        'color': '#3498db',
        'x': 5
    },
    {
        'title': 'Scenario 3: Coverage Gap',
        'icon': '❌',
        'description': 'Charter/private schools missing\nDaycares not included\nSpecialized institutions excluded',
        'implication': 'Real school-zone enforcement\nUndercounted in statistics',
        'color': '#e74c3c',
        'x': 8.5
    }
]

for scenario in scenarios:
    y_top = 8.2
    ax.add_patch(FancyBboxPatch((scenario['x']-1.4, y_top-3.2), 2.8, 3.2,
                                boxstyle="round,pad=0.1",
                                edgecolor=scenario['color'], facecolor=scenario['color'],
                                alpha=0.15, linewidth=2))

    # Title with icon
    ax.text(scenario['x'], y_top-0.3, scenario['icon'] + '  ' + scenario['title'],
            fontsize=10, fontweight='bold', ha='center')

    # Description
    ax.text(scenario['x'], y_top-1.2, scenario['description'],
            fontsize=8, ha='center', va='top', style='italic')

    # Implication box
    ax.add_patch(FancyBboxPatch((scenario['x']-1.3, y_top-3), 2.6, 0.6,
                                edgecolor=scenario['color'], facecolor='white', linewidth=1.5))
    ax.text(scenario['x'], y_top-2.7, scenario['implication'],
            fontsize=8, ha='center', va='center', fontweight='bold', color=scenario['color'])

# Bottom: What to do
y_bottom = 3.5
ax.add_patch(FancyBboxPatch((0.2, y_bottom-2.8), 9.6, 2.8,
                            boxstyle="round,pad=0.1",
                            edgecolor='#27ae60', facecolor='#d5f4e6', linewidth=2.5))

ax.text(5, y_bottom-0.3, '✅ HOW TO RESOLVE THIS', fontsize=12, fontweight='bold', ha='center', color='#27ae60')

steps = [
    '1️⃣ Tell us: Which 5 cameras in Ward 5? (location + school names)',
    '2️⃣ We search: Do those schools exist in our database?',
    '3️⃣ If missing: School data needs update',
    '4️⃣ If present: Compare coordinates with Google Maps Street View',
    '5️⃣ Result: Identify and fix root cause(s)'
]

y = y_bottom - 0.7
for step in steps:
    ax.text(0.5, y, step, fontsize=8, ha='left', va='top')
    y -= 0.35

# Note at bottom
ax.text(5, 0.3, '📌 Your observations are VALID. The data shows gaps, not errors in your memory.',
        fontsize=9, ha='center', style='italic', fontweight='bold',
        bbox=dict(boxstyle='round', facecolor='#ffffcc', alpha=0.8))

plt.tight_layout()
plt.savefig('visualizations/06_what_it_means.png', dpi=300, bbox_inches='tight', facecolor='white')
print("✓ Saved: 06_what_it_means.png")
plt.close()

print("\n✅ Summary visualizations complete!")
print("\nAll files created:")
print("  • visualizations/01_data_quality_assessment.png")
print("  • visualizations/02_distance_distribution.png")
print("  • visualizations/03_distance_bands.png")
print("  • visualizations/04_closest_cameras.png")
print("  • visualizations/05_discrepancy_summary.png")
print("  • visualizations/06_what_it_means.png")
print("\nDocumentation:")
print("  • analysis/DATA_DISCREPANCY_EXPLAINED.md")
print("  • analysis/DISCREPANCY_ANALYSIS.md")
print("  • analysis/discrepancy_analysis_data.json")
