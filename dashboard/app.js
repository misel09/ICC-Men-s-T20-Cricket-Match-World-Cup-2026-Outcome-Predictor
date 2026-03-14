// Global Chart Settings tailored for the dark premium UI
Chart.defaults.color = '#94a3b8';
Chart.defaults.font.family = "'Outfit', sans-serif";
Chart.defaults.font.size = 12;

// Customizing Tooltips for Glassmorphism feel
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(11, 13, 23, 0.95)';
Chart.defaults.plugins.tooltip.titleColor = '#00f2fe';
Chart.defaults.plugins.tooltip.titleFont = { family: "'Rajdhani', sans-serif", size: 16, weight: 'bold' };
Chart.defaults.plugins.tooltip.bodyFont = { family: "'Outfit', sans-serif", size: 14 };
Chart.defaults.plugins.tooltip.padding = 14;
Chart.defaults.plugins.tooltip.borderColor = 'rgba(0, 242, 254, 0.3)';
Chart.defaults.plugins.tooltip.borderWidth = 1;
Chart.defaults.plugins.tooltip.cornerRadius = 8;
Chart.defaults.plugins.tooltip.boxPadding = 6;

// Shared Color Palette reflecting styles.css
const colors = {
    primary: 'rgba(0, 242, 254, 0.8)',
    primaryBorder: '#00f2fe',
    primaryShadow: 'rgba(0, 242, 254, 0.3)',
    
    secondary: 'rgba(245, 158, 11, 0.8)',
    secondaryBorder: '#f59e0b',
    secondaryShadow: 'rgba(245, 158, 11, 0.3)',
    
    tertiary: 'rgba(16, 185, 129, 0.8)',
    tertiaryBorder: '#10b981',
    tertiaryShadow: 'rgba(16, 185, 129, 0.3)',

    danger: 'rgba(239, 68, 68, 0.8)',
    dangerBorder: '#ef4444',
    
    gridLines: 'rgba(255, 255, 255, 0.05)',
    gridLinesStrong: 'rgba(255, 255, 255, 0.1)'
};

// Common Grid Configuration
const gridConfig = {
    color: colors.gridLines,
    drawBorder: false,
};

const API_BASE_URL = 'http://localhost:8000/api';

document.addEventListener('DOMContentLoaded', async () => {

    // --- 1. Tab Navigation Logic ---
    const tabs = document.querySelectorAll('.nav-links li');
    const contents = document.querySelectorAll('.tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));

            tab.classList.add('active');
            document.getElementById(tab.dataset.tab).classList.add('active');
        });
    });

    // --- 2. Chart Configurations & Renderings ---
    try {
        // Fetch all data from the API concurrently
        const [overviewResponse, venueOverviewResponse, battersResponse, bowlersResponse, h2hResponse, venuesResponse] = await Promise.all([
            fetch(`${API_BASE_URL}/overview`).catch(() => null),
            fetch(`${API_BASE_URL}/venues/overview`).catch(() => null),
            fetch(`${API_BASE_URL}/batters/radar`).catch(() => null),
            fetch(`${API_BASE_URL}/bowlers/scatter`).catch(() => null),
            fetch(`${API_BASE_URL}/teams/h2h`).catch(() => null),
            fetch(`${API_BASE_URL}/venues/deepdive`).catch(() => null),
        ]);

        const overviewData = overviewResponse ? await overviewResponse.json() : null;
        const venueOverviewData = venueOverviewResponse ? await venueOverviewResponse.json() : null;
        const battersData = battersResponse ? await battersResponse.json() : null;
        const bowlersData = bowlersResponse ? await bowlersResponse.json() : null;
        const h2hData = h2hResponse ? await h2hResponse.json() : null;
        const venuesData = venuesResponse ? await venuesResponse.json() : null;


        // A) Overview Chart: Linear Growth (Matches vw_tournament_stats)
        if (overviewData) {
            const ctxOverview = document.getElementById('overviewChart').getContext('2d');
            new Chart(ctxOverview, {
                type: 'line',
                data: {
                    labels: overviewData.labels,
                    datasets: [
                        {
                            label: 'Calculated Avg Team Total',
                            data: overviewData.data,
                            borderColor: colors.primaryBorder,
                            backgroundColor: (context) => {
                                const ctx = context.chart.ctx;
                                const gradient = ctx.createLinearGradient(0, 0, 0, 400);
                                gradient.addColorStop(0, 'rgba(0, 242, 254, 0.4)');
                                gradient.addColorStop(1, 'rgba(0, 242, 254, 0.0)');
                                return gradient;
                            },
                            borderWidth: 4,
                            tension: 0.4, // Smooth Spline curve
                            fill: true,
                            pointBackgroundColor: '#0b0d17',
                            pointBorderColor: colors.primaryBorder,
                            pointBorderWidth: 3,
                            pointRadius: 6,
                            pointHoverRadius: 8
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    scales: {
                        y: { grid: gridConfig, border: {display: false} },
                        x: { grid: { display: false }, border: {display: false} }
                    },
                    plugins: { legend: { display: false } }
                }
            });
        }

        // B) Venue Quick View: Avg 2nd Innings Score
        if (venueOverviewData) {
            const ctxVenueOverview = document.getElementById('venueOverviewChart').getContext('2d');
            new Chart(ctxVenueOverview, {
                type: 'bar',
                data: {
                    labels: venueOverviewData.labels,
                    datasets: [{
                        label: 'Avg 2nd Inn Avg',
                        data: venueOverviewData.data,
                        backgroundColor: [
                            colors.primary, 
                            colors.secondary,
                            colors.tertiary,
                            colors.danger
                        ],
                        borderWidth: 0,
                        borderRadius: 8,
                        barPercentage: 0.6
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: { grid: gridConfig, beginAtZero: false, min: 140, border: {display: false} },
                        x: { grid: { display: false }, border: {display: false} }
                    },
                    plugins: { legend: { display: false } }
                }
            });
        }

        // C) Batter Radar Chart
        if (battersData && battersData.length >= 2) {
            const ctxRadar = document.getElementById('batterRadarChart').getContext('2d');
            new Chart(ctxRadar, {
                type: 'radar',
                data: {
                    labels: ['Strike Rate', 'Average', 'Boundary %', 'Consistency Mod.', 'Pace Dominance', 'Spin Dominance'],
                    datasets: [
                        {
                            label: `${battersData[0].name} (Full Match)`,
                            data: battersData[0].stats,
                            backgroundColor: 'rgba(0, 242, 254, 0.25)',
                            borderColor: colors.primaryBorder,
                            pointBackgroundColor: colors.primaryBorder,
                            borderWidth: 2,
                            pointRadius: 4
                        },
                        {
                            label: `${battersData[1].name} (Full Match)`,
                            data: battersData[1].stats,
                            backgroundColor: 'rgba(245, 158, 11, 0.25)',
                            borderColor: colors.secondaryBorder,
                            pointBackgroundColor: colors.secondaryBorder,
                            borderWidth: 2,
                            pointRadius: 4
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        r: {
                            angleLines: { color: colors.gridLinesStrong },
                            grid: { color: colors.gridLines },
                            pointLabels: { color: '#f0f2f5', font: { size: 12, family: "'Rajdhani', sans-serif" } },
                            ticks: { display: false, backdropColor: 'transparent' }
                        }
                    },
                    plugins: {
                        legend: { position: 'bottom', labels: { color: '#f0f2f5', padding: 20 } }
                    }
                }
            });
        }

        // D) Bowler Scatter: Outliers
        if (bowlersData) {
            const ctxScatter = document.getElementById('bowlerScatterChart').getContext('2d');
            new Chart(ctxScatter, {
                type: 'scatter',
                data: {
                    datasets: [
                        {
                            label: 'Key Spinners',
                            data: bowlersData.spinners,
                            backgroundColor: colors.primary,
                            borderColor: colors.primaryBorder,
                            borderWidth: 2,
                            pointRadius: 8,
                            pointHoverRadius: 12
                        },
                        {
                            label: 'Lethal Pace',
                            data: bowlersData.pacers,
                            backgroundColor: colors.danger,
                            borderColor: colors.dangerBorder,
                            borderWidth: 2,
                            pointRadius: 8,
                            pointHoverRadius: 12
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { 
                            title: { display: true, text: 'Economy Rate (Muted runs / Over)', color: '#94a3b8' },
                            grid: gridConfig, border: {display: false},
                            reverse: true
                        },
                        y: { 
                            title: { display: true, text: 'Strike Rate (Balls / Wicket)', color: '#94a3b8' },
                            grid: gridConfig, border: {display: false},
                            reverse: true
                        }
                    },
                    plugins: {
                        legend: { position: 'top' },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) {
                                    return `${ctx.raw.name}: Eco ${ctx.raw.x}, SR ${ctx.raw.y}`;
                                }
                            }
                        }
                    }
                }
            });
        }

        // E) Head to Head: Squads Filtered
        if (h2hData) {
            const ctxH2H = document.getElementById('h2hBarChart').getContext('2d');
            new Chart(ctxH2H, {
                type: 'bar',
                data: {
                    labels: h2hData.labels,
                    datasets: [
                        {
                            label: 'Team 1 Wins',
                            data: h2hData.team1_wins,
                            backgroundColor: colors.primaryBorder,
                            borderRadius: 6,
                            barPercentage: 0.5
                        },
                        {
                            label: 'Team 2 Wins',
                            data: h2hData.team2_wins,
                            backgroundColor: colors.secondaryBorder,
                            borderRadius: 6,
                            barPercentage: 0.5
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { stacked: true, grid: { display: false }, border: {display: false} },
                        y: { stacked: true, grid: gridConfig, border: {display: false} }
                    },
                    plugins: {
                        legend: { position: 'top' }
                    }
                }
            });
        }
        
        // F) Venue Deep Dive
        if (venuesData) {
            const ctxVenueLine = document.getElementById('venueLineChart').getContext('2d');
            new Chart(ctxVenueLine, {
                type: 'bar', // Using grouped bar for clearer innings comparison
                data: {
                    labels: venuesData.labels,
                    datasets: [
                        {
                            label: 'Avg 1st Innings Total',
                            data: venuesData.first_inn,
                            backgroundColor: colors.primary,
                            borderRadius: 4
                        },
                        {
                            label: 'Avg 2nd Innings Target (Chased)',
                            data: venuesData.second_inn,
                            backgroundColor: colors.tertiary,
                            borderRadius: 4
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: { grid: gridConfig, border: {display: false}, min: 130 },
                        x: { grid: { display: false }, border: {display: false} }
                    },
                    plugins: {
                        legend: { position: 'top' }
                    }
                }
            });
        }

    } catch (e) {
        console.error("Failed to fetch dashboard data. Make sure backend is running at localhost:8000/api", e);
    }
});
