import streamlit as st
import duckdb
import pandas as pd
import altair as alt

st.write("Hello World")

ddb = duckdb.connect("/workspaces/talento_tech/data/transformed/db.duckdb")

top_valle_exports = ddb.sql(
    """SELECT POSAR, TOTAL_FOBPES
    FROM top_valle_exports
    LIMIT 10;"""
).df()

top_valle_exports_to_korea = ddb.sql(
    """SELECT POSAR, TOTAL_FOBPES
    FROM top_valle_exports_to_korea
    LIMIT 10;"""
).df()

cluster_valle_world_exports = ddb.sql(
    """SELECT POSAR, FOBPES, kmeans
    FROM cluster_valle_world_exports"""
).df()

chart = (
    alt.Chart(cluster_valle_world_exports)
    .mark_circle()
    .encode(x="POSAR", y="FOBPES", color="kmeans", tooltip=["POSAR", "FOBPES", "kmeans"])
)

st.bar_chart(top_valle_exports, x="POSAR", y="TOTAL_FOBPES")

st.bar_chart(top_valle_exports_to_korea, x="POSAR", y="TOTAL_FOBPES")

st.altair_chart(chart)