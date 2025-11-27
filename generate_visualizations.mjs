#!/usr/bin/env node

/**
 * Enforcement Rhythms Visualization Generator
 * Creates PNG charts from the enforcement_rhythms_report.json
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createCanvas } from 'canvas';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load the analysis report
const reportPath = path.join(__dirname, 'output', 'enforcement_rhythms_report.json');
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));

const outputDir = path.join(__dirname, 'docs', 'analysis');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

// Color palette
const colors = {
  primary: '#2563eb',
  secondary: '#7c3aed',
  accent: '#dc2626',
  success: '#16a34a',
  warning: '#ea580c',
  neutral: '#6b7280',
  background: '#ffffff',
  grid: '#e5e7eb',
  text: '#1f2937'
};

/**
 * Draw Daily Patterns Bar Chart
 */
function generateDailyPatternChart() {
  const canvas = createCanvas(1000, 600);
  const ctx = canvas.getContext('2d');

  const daily = report.analyses.daily;
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

  // Background
  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, 1000, 600);

  // Title
  ctx.fillStyle = colors.text;
  ctx.font = 'bold 28px Arial';
  ctx.fillText('Daily Enforcement Patterns (2008-2024)', 50, 40);

  // Subtitle
  ctx.font = '14px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.fillText('Total tickets by day of week across 17 years', 50, 65);

  // Find max for scaling
  const maxTickets = Math.max(...daily.map(d => d.ticket_count));
  const barWidth = 100;
  const chartHeight = 400;
  const chartTop = 120;

  // Draw bars
  daily.forEach((day, idx) => {
    const barHeight = (day.ticket_count / maxTickets) * chartHeight;
    const x = 80 + idx * 120;
    const y = chartTop + chartHeight - barHeight;

    // Bar
    ctx.fillStyle = idx === 0 || idx === 6 ? colors.secondary : colors.primary;
    ctx.fillRect(x, y, barWidth, barHeight);

    // Value on top
    ctx.fillStyle = colors.text;
    ctx.font = 'bold 12px Arial';
    ctx.textAlign = 'center';
    ctx.fillText(`${(day.ticket_count / 1e6).toFixed(1)}M`, x + barWidth / 2, y - 10);

    // Day label
    ctx.font = '12px Arial';
    ctx.fillStyle = colors.text;
    ctx.fillText(dayNames[idx], x + barWidth / 2, chartTop + chartHeight + 25);

    // Percentage
    ctx.font = '11px Arial';
    ctx.fillStyle = colors.neutral;
    const pct = ((day.ticket_count / 37e6) * 100).toFixed(1);
    ctx.fillText(`${pct}%`, x + barWidth / 2, chartTop + chartHeight + 45);
  });

  // Legend
  ctx.font = '12px Arial';
  ctx.fillStyle = colors.primary;
  ctx.fillRect(50, 550, 15, 15);
  ctx.fillStyle = colors.text;
  ctx.textAlign = 'left';
  ctx.fillText('Weekday', 70, 562);

  ctx.fillStyle = colors.secondary;
  ctx.fillRect(250, 550, 15, 15);
  ctx.fillStyle = colors.text;
  ctx.fillText('Weekend', 270, 562);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(path.join(outputDir, 'daily-patterns.png'), buffer);
  console.log('✅ Generated: daily-patterns.png');
}

/**
 * Draw Seasonal Patterns Line Chart
 */
function generateSeasonalPatternChart() {
  const canvas = createCanvas(1200, 600);
  const ctx = canvas.getContext('2d');

  const seasonal = report.analyses.seasonal;

  // Background
  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, 1200, 600);

  // Title
  ctx.fillStyle = colors.text;
  ctx.font = 'bold 28px Arial';
  ctx.fillText('Seasonal Enforcement Patterns (2008-2024)', 50, 40);

  ctx.font = '14px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.fillText('Monthly ticket counts showing seasonal trends', 50, 65);

  const chartLeft = 80;
  const chartTop = 120;
  const chartWidth = 1050;
  const chartHeight = 400;

  // Draw grid
  ctx.strokeStyle = colors.grid;
  ctx.lineWidth = 1;
  for (let i = 0; i <= 10; i++) {
    const y = chartTop + (i * chartHeight / 10);
    ctx.beginPath();
    ctx.moveTo(chartLeft, y);
    ctx.lineTo(chartLeft + chartWidth, y);
    ctx.stroke();
  }

  // Find max for scaling
  const maxTickets = Math.max(...seasonal.map(s => s.ticket_count));
  const minTickets = Math.min(...seasonal.map(s => s.ticket_count));
  const range = maxTickets - minTickets;

  // Draw line chart
  ctx.strokeStyle = colors.primary;
  ctx.lineWidth = 3;
  ctx.beginPath();

  seasonal.forEach((month, idx) => {
    const x = chartLeft + (idx / seasonal.length) * chartWidth;
    const normalizedValue = (month.ticket_count - minTickets) / range;
    const y = chartTop + chartHeight - (normalizedValue * chartHeight);

    if (idx === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  // Draw points
  seasonal.forEach((month, idx) => {
    const x = chartLeft + (idx / seasonal.length) * chartWidth;
    const normalizedValue = (month.ticket_count - minTickets) / range;
    const y = chartTop + chartHeight - (normalizedValue * chartHeight);

    ctx.fillStyle = colors.primary;
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  // Y-axis labels
  ctx.font = '11px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.textAlign = 'right';
  for (let i = 0; i <= 10; i++) {
    const value = minTickets + (i / 10) * range;
    const y = chartTop + chartHeight - (i * chartHeight / 10);
    ctx.fillText(`${(value / 1e3).toFixed(0)}k`, chartLeft - 10, y + 4);
  }

  // X-axis labels (sample every 12 months)
  ctx.font = '10px Arial';
  ctx.fillStyle = colors.text;
  ctx.textAlign = 'center';
  for (let i = 0; i < seasonal.length; i += 12) {
    const x = chartLeft + (i / seasonal.length) * chartWidth;
    const monthLabel = seasonal[i].month.split('-')[0];
    ctx.fillText(monthLabel, x, chartTop + chartHeight + 20);
  }

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(path.join(outputDir, 'seasonal-patterns.png'), buffer);
  console.log('✅ Generated: seasonal-patterns.png');
}

/**
 * Draw Anomalies Scatter Chart
 */
function generateAnomaliesChart() {
  const canvas = createCanvas(1000, 600);
  const ctx = canvas.getContext('2d');

  const anomalies = report.analyses.anomalies;

  // Background
  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, 1000, 600);

  // Title
  ctx.fillStyle = colors.text;
  ctx.font = 'bold 28px Arial';
  ctx.fillText('Enforcement Anomalies (Z-Score Analysis)', 50, 40);

  ctx.font = '14px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.fillText('Unusual enforcement days detected (2008-2024)', 50, 65);

  const chartLeft = 100;
  const chartTop = 120;
  const chartWidth = 850;
  const chartHeight = 380;

  // Draw axes
  ctx.strokeStyle = colors.text;
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(chartLeft, chartTop);
  ctx.lineTo(chartLeft, chartTop + chartHeight);
  ctx.lineTo(chartLeft + chartWidth, chartTop + chartHeight);
  ctx.stroke();

  // Draw grid
  ctx.strokeStyle = colors.grid;
  ctx.lineWidth = 1;
  for (let i = 1; i <= 10; i++) {
    const x = chartLeft + (i / 10) * chartWidth;
    ctx.beginPath();
    ctx.moveTo(x, chartTop);
    ctx.lineTo(x, chartTop + chartHeight);
    ctx.stroke();
  }

  // Draw center line (Z=0)
  const centerY = chartTop + chartHeight / 2;
  ctx.strokeStyle = colors.neutral;
  ctx.lineWidth = 2;
  ctx.setLineDash([5, 5]);
  ctx.beginPath();
  ctx.moveTo(chartLeft, centerY);
  ctx.lineTo(chartLeft + chartWidth, centerY);
  ctx.stroke();
  ctx.setLineDash([]);

  // Draw anomaly points
  anomalies.forEach((anom, idx) => {
    const x = chartLeft + (idx / anomalies.length) * chartWidth;
    const zScore = parseFloat(anom.z_score);
    const yOffset = (zScore / 4) * (chartHeight / 2); // Scale z-score
    const y = centerY - yOffset;

    const color = Math.abs(zScore) > 3 ? colors.accent : colors.warning;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fill();
  });

  // Y-axis labels
  ctx.font = '11px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.textAlign = 'right';
  for (let i = -4; i <= 4; i += 2) {
    const y = centerY - (i / 4) * (chartHeight / 2);
    ctx.fillText(`${i.toFixed(1)}σ`, chartLeft - 15, y + 4);
  }

  // X-axis label
  ctx.font = '12px Arial';
  ctx.fillStyle = colors.text;
  ctx.textAlign = 'center';
  ctx.fillText('Time →', chartLeft + chartWidth + 30, chartTop + chartHeight + 10);

  // Y-axis label
  ctx.save();
  ctx.translate(30, chartTop + chartHeight / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.fillText('Z-Score (Standard Deviations)', 0, 0);
  ctx.restore();

  // Legend
  ctx.font = '12px Arial';
  ctx.fillStyle = colors.accent;
  ctx.fillRect(50, 540, 15, 15);
  ctx.fillStyle = colors.text;
  ctx.textAlign = 'left';
  ctx.fillText('Severe (|Z| > 3)', 70, 552);

  ctx.fillStyle = colors.warning;
  ctx.fillRect(280, 540, 15, 15);
  ctx.fillStyle = colors.text;
  ctx.fillText('Moderate (|Z| > 2)', 300, 552);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(path.join(outputDir, 'anomalies.png'), buffer);
  console.log('✅ Generated: anomalies.png');
}

/**
 * Draw Enforcement Intensity Heatmap
 */
function generateIntensityHeatmap() {
  const canvas = createCanvas(1100, 500);
  const ctx = canvas.getContext('2d');

  // Background
  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, 1100, 500);

  // Title
  ctx.fillStyle = colors.text;
  ctx.font = 'bold 28px Arial';
  ctx.fillText('Enforcement Intensity Heatmap (2008-2024)', 50, 40);

  ctx.font = '14px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.fillText('Relative ticket volume by month and year', 50, 65);

  const seasonal = report.analyses.seasonal;
  const cellWidth = 40;
  const cellHeight = 30;
  const startX = 100;
  const startY = 120;

  // Group by year and month
  const heatData = {};
  seasonal.forEach(month => {
    const [year, monthNum] = month.month.split('-');
    if (!heatData[year]) heatData[year] = {};
    heatData[year][parseInt(monthNum)] = month.ticket_count;
  });

  // Find min/max for color scaling
  const allValues = seasonal.map(s => s.ticket_count);
  const minVal = Math.min(...allValues);
  const maxVal = Math.max(...allValues);

  // Helper to get color based on intensity
  const getHeatColor = (value) => {
    const normalized = (value - minVal) / (maxVal - minVal);
    if (normalized > 0.75) return '#dc2626'; // Red
    if (normalized > 0.5) return '#ea580c'; // Orange
    if (normalized > 0.25) return '#eab308'; // Yellow
    return '#86efac'; // Green
  };

  // Draw heatmap
  const years = Object.keys(heatData).sort();
  years.forEach((year, yIdx) => {
    for (let month = 1; month <= 12; month++) {
      const value = heatData[year][month];
      if (value !== undefined) {
        const x = startX + (month - 1) * cellWidth;
        const y = startY + yIdx * cellHeight;

        ctx.fillStyle = getHeatColor(value);
        ctx.fillRect(x, y, cellWidth - 1, cellHeight - 1);

        // Border
        ctx.strokeStyle = colors.grid;
        ctx.lineWidth = 1;
        ctx.strokeRect(x, y, cellWidth - 1, cellHeight - 1);
      }
    }

    // Year labels
    ctx.font = '11px Arial';
    ctx.fillStyle = colors.text;
    ctx.textAlign = 'right';
    ctx.fillText(year, startX - 10, startY + yIdx * cellHeight + 20);
  });

  // Month headers
  const monthNames = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];
  ctx.font = '11px Arial';
  ctx.fillStyle = colors.text;
  ctx.textAlign = 'center';
  monthNames.forEach((m, idx) => {
    const x = startX + idx * cellWidth + cellWidth / 2;
    ctx.fillText(m, x, startY - 10);
  });

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(path.join(outputDir, 'intensity-heatmap.png'), buffer);
  console.log('✅ Generated: intensity-heatmap.png');
}

/**
 * Draw Fine Amounts Comparison
 */
function generateFineAmountsChart() {
  const canvas = createCanvas(900, 500);
  const ctx = canvas.getContext('2d');

  const daily = report.analyses.daily;

  // Background
  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, 900, 500);

  // Title
  ctx.fillStyle = colors.text;
  ctx.font = 'bold 28px Arial';
  ctx.fillText('Average Fine Amount by Day (2008-2024)', 50, 40);

  ctx.font = '14px Arial';
  ctx.fillStyle = colors.neutral;
  ctx.fillText('Weekday vs Weekend fine differences', 50, 65);

  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const barWidth = 80;
  const chartHeight = 300;
  const chartTop = 120;

  // Find max fine
  const maxFine = Math.max(...daily.map(d => d.avg_fine));

  // Draw bars
  daily.forEach((day, idx) => {
    const avgFine = parseFloat(day.avg_fine);
    const barHeight = (avgFine / maxFine) * chartHeight;
    const x = 80 + idx * 100;
    const y = chartTop + chartHeight - barHeight;

    // Bar
    ctx.fillStyle = idx === 0 || idx === 6 ? colors.secondary : colors.primary;
    ctx.fillRect(x, y, barWidth, barHeight);

    // Value on top
    ctx.fillStyle = colors.text;
    ctx.font = 'bold 13px Arial';
    ctx.textAlign = 'center';
    ctx.fillText(`$${avgFine.toFixed(2)}`, x + barWidth / 2, y - 10);

    // Day label
    ctx.font = '12px Arial';
    ctx.fillStyle = colors.text;
    ctx.fillText(dayNames[idx], x + barWidth / 2, chartTop + chartHeight + 25);
  });

  // Weekday/Weekend average lines
  const weekdayFines = daily.slice(1, 6).map(d => d.avg_fine);
  const weekendFines = [daily[0].avg_fine, daily[6].avg_fine];
  const weekdayAvg = weekdayFines.reduce((a, b) => a + b) / weekdayFines.length;
  const weekendAvg = weekendFines.reduce((a, b) => a + b) / weekendFines.length;

  // Stats box
  ctx.font = '12px Arial';
  ctx.fillStyle = colors.primary;
  ctx.fillRect(50, 430, 300, 50);
  ctx.fillStyle = colors.background;
  ctx.fillText(`Weekday Avg: $${weekdayAvg.toFixed(2)}`, 60, 450);
  ctx.fillText(`Weekend Avg: $${weekendAvg.toFixed(2)} (${(((weekdayAvg - weekendAvg) / weekendAvg * 100).toFixed(1))}% less)`, 60, 470);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(path.join(outputDir, 'fine-amounts.png'), buffer);
  console.log('✅ Generated: fine-amounts.png');
}

/**
 * Main execution
 */
async function main() {
  try {
    console.log('🎨 Generating enforcement rhythms visualizations...\n');

    generateDailyPatternChart();
    generateSeasonalPatternChart();
    generateAnomaliesChart();
    generateIntensityHeatmap();
    generateFineAmountsChart();

    console.log('\n✨ All visualizations generated successfully!');
    console.log(`📁 Saved to: ${outputDir}`);
  } catch (err) {
    console.error('❌ Error generating visualizations:', err);
    process.exit(1);
  }
}

main();
