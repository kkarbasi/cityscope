from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Config & data loading
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[3] / "data" / "urban_research.db"
LOGO_PATH = Path(__file__).resolve().parent / "logo.svg"


@st.cache_resource
def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300)
def load_all_data() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        """
        SELECT d.geo_id, d.metric, d.year, d.value, d.source,
               g.name, g.geo_type, g.population
        FROM data_points d
        JOIN geographies g ON d.geo_id = g.geo_id
        ORDER BY d.year, g.name
        """,
        conn,
    )


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Urban Research Dashboard",
    page_icon=":city_sunrise:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

df_all = load_all_data()

if df_all.empty:
    st.error("No data found. Run `uv run urban-research fetch --all` first.")
    st.stop()

all_metrics = sorted(df_all["metric"].unique())
all_years = sorted(df_all["year"].unique())

# ---------------------------------------------------------------------------
# Sidebar: logo + filters
# ---------------------------------------------------------------------------

st.sidebar.image(str(LOGO_PATH), use_container_width=True)
st.sidebar.divider()
st.sidebar.subheader("Filters")

geo_type_options = {"All": None, "Metro Areas": "metro", "Cities": "city"}
geo_type_label = st.sidebar.radio("Geography type", list(geo_type_options.keys()), index=0)
geo_type_filter = geo_type_options[geo_type_label]

year_range = st.sidebar.select_slider(
    "Year range",
    options=all_years,
    value=(min(all_years), max(all_years)),
)

min_pop = st.sidebar.number_input(
    "Minimum population",
    min_value=0,
    max_value=10_000_000,
    value=200_000,
    step=50_000,
    format="%d",
)

top_n = st.sidebar.slider("Top N results", min_value=5, max_value=100, value=25)

# Apply base filters
df = df_all.copy()
if geo_type_filter:
    df = df[df["geo_type"] == geo_type_filter]
df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]
if min_pop > 0:
    df = df[df["population"] >= min_pop]

latest_year = df["year"].max() if not df.empty else 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

METRIC_LABELS = {
    "population": "Population",
    "population_change": "Pop. Change",
    "population_change_pct": "Pop. Growth %",
    "employment": "Employment",
    "employment_change": "Job Change",
    "employment_change_pct": "Job Growth %",
    "unemployment_rate": "Unemployment Rate %",
    "avg_annual_pay": "Avg. Annual Pay",
    "avg_weekly_wage": "Avg. Weekly Wage",
}

RANKING_METRICS = [
    "population_change_pct",
    "employment_change_pct",
    "unemployment_rate",
    "avg_annual_pay",
]

TREND_METRICS = {
    "Population": "population",
    "Population Growth %": "population_change_pct",
    "Employment": "employment",
    "Job Growth %": "employment_change_pct",
    "Unemployment Rate %": "unemployment_rate",
    "Avg. Annual Pay": "avg_annual_pay",
    "Avg. Weekly Wage": "avg_weekly_wage",
}


def fmt_value(val: float, metric: str) -> str:
    if "pct" in metric or metric == "unemployment_rate":
        return f"{val:+.2f}%" if "change" in metric else f"{val:.1f}%"
    if metric in ("population", "employment", "population_change", "employment_change"):
        return f"{val:,.0f}"
    if metric in ("avg_annual_pay", "avg_weekly_wage", "total_wages"):
        return f"${val:,.0f}"
    return f"{val:,.2f}"


# ---------------------------------------------------------------------------
# Header metrics
# ---------------------------------------------------------------------------

st.image(str(LOGO_PATH), width=360)

n_geos = df["geo_id"].nunique()
df_pop_growth = df[(df["metric"] == "population_change_pct") & (df["year"] == latest_year)]
df_job_growth = df[(df["metric"] == "employment_change_pct") & (df["year"] == latest_year)]

cols = st.columns(4)
cols[0].metric("Geographies", f"{n_geos:,}")
cols[1].metric("Data Points", f"{len(df):,}")

if not df_pop_growth.empty:
    fastest = df_pop_growth.loc[df_pop_growth["value"].idxmax()]
    cols[2].metric("Fastest Pop. Growth", fastest["name"][:28], f"{fastest['value']:+.2f}%")

if not df_job_growth.empty:
    best_jobs = df_job_growth.loc[df_job_growth["value"].idxmax()]
    cols[3].metric("Fastest Job Growth", best_jobs["name"][:28], f"{best_jobs['value']:+.1f}%")

st.divider()


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_rankings, tab_trends, tab_compare, tab_explorer = st.tabs(
    ["Rankings", "Trends", "City Profile", "Data Explorer"]
)


# ---- Tab 1: Rankings ----
with tab_rankings:
    available_ranking = [m for m in RANKING_METRICS if m in df["metric"].values]
    if not available_ranking:
        st.info("No ranking data available.")
    else:
        rank_labels = {METRIC_LABELS.get(m, m): m for m in available_ranking}
        rank_choice = st.selectbox("Rank by", list(rank_labels.keys()), index=0)
        rank_metric = rank_labels[rank_choice]

        rank_year = st.select_slider(
            "Year",
            options=sorted(df[df["metric"] == rank_metric]["year"].unique()),
            value=df[df["metric"] == rank_metric]["year"].max(),
            key="rank_year",
        )

        df_rank = df[(df["metric"] == rank_metric) & (df["year"] == rank_year)]

        if df_rank.empty:
            st.info("No data for this metric/year combination.")
        else:
            ascending = rank_metric == "unemployment_rate"
            sorted_df = df_rank.sort_values("value", ascending=ascending).head(top_n)

            # For unemployment, lower is better → green-to-red
            # For growth, higher is better → red-to-green
            if ascending:
                colors = ["#22c55e", "#fbbf24", "#ef4444"]
            else:
                colors = ["#ef4444", "#fbbf24", "#22c55e"]

            fig = px.bar(
                sorted_df,
                x="value",
                y="name",
                orientation="h",
                color="value",
                color_continuous_scale=colors,
                labels={"value": rank_choice, "name": ""},
                hover_data={"population": ":,", "geo_type": True},
            )
            fig.update_layout(
                yaxis={"categoryorder": "total ascending" if not ascending else "total descending",
                       "tickfont": {"size": 11}},
                height=max(400, top_n * 28),
                margin={"l": 0, "r": 20, "t": 10, "b": 40},
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Full Table")
            tbl = (
                df_rank[["name", "geo_type", "value", "population"]]
                .sort_values("value", ascending=ascending)
                .reset_index(drop=True)
            )
            tbl.index += 1
            tbl.columns = ["Geography", "Type", rank_choice, "Population"]
            tbl["Population"] = tbl["Population"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
            tbl[rank_choice] = tbl[rank_choice].apply(lambda v: fmt_value(v, rank_metric))
            st.dataframe(tbl, use_container_width=True, height=600)


# ---- Tab 2: Trends ----
with tab_trends:
    available_trends = {k: v for k, v in TREND_METRICS.items() if v in df["metric"].values}
    if not available_trends:
        st.info("No trend data available.")
    else:
        trend_choice = st.selectbox("Metric", list(available_trends.keys()), index=0)
        trend_metric = available_trends[trend_choice]

        df_trend = df[df["metric"] == trend_metric]
        available_names = sorted(df_trend["name"].unique())

        # Smart defaults: top 5 by latest year value
        df_latest = df_trend[df_trend["year"] == df_trend["year"].max()]
        if "change" in trend_metric or "growth" in trend_metric.lower():
            defaults = df_latest.sort_values("value", ascending=False).head(5)["name"].tolist()
        else:
            defaults = df_latest.sort_values("value", ascending=False).head(5)["name"].tolist()
        defaults = [n for n in defaults if n in available_names][:5]

        selected = st.multiselect(
            "Select geographies to compare",
            options=available_names,
            default=defaults,
            max_selections=15,
            key="trend_select",
        )

        if selected:
            df_sel = df_trend[df_trend["name"].isin(selected)]

            fig = px.line(
                df_sel, x="year", y="value", color="name",
                markers=True,
                labels={"value": trend_choice, "year": "Year", "name": ""},
            )

            yformat = {}
            if "pct" in trend_metric or "rate" in trend_metric:
                yformat = {"ticksuffix": "%"}
            elif "pay" in trend_metric or "wage" in trend_metric:
                yformat = {"tickprefix": "$", "tickformat": ","}
            else:
                yformat = {"tickformat": ","}

            fig.update_layout(
                height=500,
                yaxis=yformat,
                xaxis={"dtick": 1},
                legend={"orientation": "h", "y": -0.2},
                margin={"t": 10},
            )
            st.plotly_chart(fig, use_container_width=True)

            # Cumulative change chart for absolute metrics
            if trend_metric in ("population", "employment"):
                st.subheader(f"Cumulative {trend_choice} Change Since {year_range[0]}")
                pivot = df_sel.pivot_table(index="year", columns="name", values="value")
                if len(pivot) > 1:
                    base = pivot.iloc[0]
                    cum = ((pivot - base) / base * 100).reset_index().melt(
                        id_vars="year", var_name="name", value_name="cumulative_pct",
                    )
                    fig_cum = px.line(
                        cum, x="year", y="cumulative_pct", color="name",
                        markers=True,
                        labels={"cumulative_pct": "Cumulative Change %", "year": "Year", "name": ""},
                    )
                    fig_cum.update_layout(
                        height=400,
                        yaxis={"ticksuffix": "%"},
                        xaxis={"dtick": 1},
                        legend={"orientation": "h", "y": -0.25},
                        margin={"t": 10},
                    )
                    fig_cum.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                    st.plotly_chart(fig_cum, use_container_width=True)
        else:
            st.info("Select one or more geographies above to see trends.")


# ---- Tab 3: City Profile ----
with tab_compare:
    st.subheader("City / Metro Profile")

    all_names = sorted(df["name"].unique())
    chosen = st.selectbox("Select a geography", all_names, index=0, key="profile_geo")

    if chosen:
        df_geo = df[df["name"] == chosen]
        geo_id = df_geo["geo_id"].iloc[0]
        geo_pop = df_geo["population"].iloc[0]

        st.markdown(f"**{chosen}** — Population: **{geo_pop:,.0f}**")

        # Summary cards for latest year
        metrics_latest = df_geo[df_geo["year"] == df_geo["year"].max()]
        card_cols = st.columns(min(len(metrics_latest), 4))
        for i, (_, row) in enumerate(metrics_latest.iterrows()):
            col = card_cols[i % len(card_cols)]
            label = METRIC_LABELS.get(row["metric"], row["metric"])
            col.metric(label, fmt_value(row["value"], row["metric"]))

        st.divider()

        # All metrics over time
        geo_metrics = sorted(df_geo["metric"].unique())
        for metric in geo_metrics:
            df_m = df_geo[df_geo["metric"] == metric].sort_values("year")
            if len(df_m) < 2:
                continue

            label = METRIC_LABELS.get(metric, metric)

            yformat = {}
            if "pct" in metric or "rate" in metric:
                yformat = {"ticksuffix": "%"}
            elif "pay" in metric or "wage" in metric:
                yformat = {"tickprefix": "$", "tickformat": ","}
            else:
                yformat = {"tickformat": ","}

            fig = px.line(
                df_m, x="year", y="value", markers=True,
                labels={"value": label, "year": ""},
                title=label,
            )
            fig.update_layout(
                height=300,
                yaxis=yformat,
                xaxis={"dtick": 1},
                margin={"t": 40, "b": 20},
                showlegend=False,
            )
            if "change" in metric:
                fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            st.plotly_chart(fig, use_container_width=True)


# ---- Tab 4: Data Explorer ----
with tab_explorer:
    st.subheader("Raw Data Explorer")

    col1, col2 = st.columns(2)
    with col1:
        metric_filter = st.selectbox(
            "Metric", options=["All"] + sorted(df["metric"].unique()), index=0,
        )
    with col2:
        source_filter = st.selectbox(
            "Source", options=["All"] + sorted(df["source"].unique()), index=0,
        )

    df_explore = df.copy()
    if metric_filter != "All":
        df_explore = df_explore[df_explore["metric"] == metric_filter]
    if source_filter != "All":
        df_explore = df_explore[df_explore["source"] == source_filter]

    display_df = (
        df_explore[["name", "geo_type", "metric", "year", "value", "population"]]
        .sort_values(["year", "name", "metric"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    display_df.columns = ["Geography", "Type", "Metric", "Year", "Value", "Population"]

    st.dataframe(display_df, use_container_width=True, height=700)

    csv = display_df.to_csv(index=False)
    st.download_button("Download as CSV", csv, "urban_research_data.csv", "text/csv")
