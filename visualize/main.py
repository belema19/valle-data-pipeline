import streamlit as st
import duckdb
import pandas as pd
import altair as alt

def main():

    ddb = duckdb.connect("/workspaces/talento_tech/data/transformed/db.duckdb")

    # Valle del Cauca
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

    cluster_valle_korea_exports = ddb.sql(
        """SELECT POSAR, FOBPES, kmeans
        FROM cluster_valle_korea_exports"""
    ).df()

    # Korea

    top_korea_imports = ddb.sql(
        """SELECT *
        FROM top_korea_imports
        LIMIT 10;"""
    ).df()

    top_korea_iberoamerica_imports = ddb.sql(
        """SELECT *
        FROM top_korea_imports_from_iberoamerica
        LIMIT 10;"""
    ).df()

    top_korea_colombia_imports = ddb.sql(
        """SELECT *
        FROM top_korea_imports_from_colombia
        LIMIT 10;"""
    ).df()

    cluster_korea_world_imports = ddb.sql(
        """SELECT *
        FROM cluster_korea_world_imports;"""
    ).df()

    cluster_korea_colombia_imports = ddb.sql(
        """SELECT *
        FROM cluster_korea_colombia_imports;"""
    ).df()

    ddb.close()

    cluster1 = (
        alt.Chart(cluster_valle_world_exports)
        .mark_circle()
        .encode(x="POSAR", y="FOBPES", color="kmeans", tooltip=["POSAR", "FOBPES", "kmeans"])
    )

    cluster2 = (
        alt.Chart(cluster_valle_korea_exports)
        .mark_circle()
        .encode(x="POSAR", y="FOBPES", color="kmeans", tooltip=["POSAR", "FOBPES", "kmeans"])
    )

    st.markdown("# OPORTUNIDADES DE EXPORTACIÓN PARA EL VALLE DEL CAUCA")

    bar1, bar2 = st.columns(2)

    bar1.markdown("## ¿Qué Exporta el Valle del Cauca?")
    bar1.markdown(
        """
        Los principales sectores son:
        - Azucares y confiteria (17).
        - Aceites esenciales y recinas; perfumería, cosméticos y artículos de aseo (33).
        - Químicos orgánicos (29).
        - Maquinaria y equipo eléctrico y sus partes (85).
        - Caucho y artículos de caucho (40).
        - Productos farmacéuticos (30).
        - Prendas y accesorios de vestir de punto (61).
        - Plástico y artículos de plástico (39).
        - Mueblería y artículos de iluminación (94).
        - Prendas y accesorios de vestir no de punto (62)."""
    )

    bar2.markdown("## ¿Qué Exporta hacia Corea del Sur?")
    bar2.markdown(
        """
        Los principales sectores son:
        - Cobre y artículos de cobre (74).
        - Papel y cartón (48).
        - Café, té, mate y especias (09).
        - Preparaciones de vegetales, frutas, nueces u otras partes de plantas (20).
        - Aluminio y artículos de aluminio (76).
        - Preparaciones comestibles (21).
        - Productos de origen animal (05).
        - Libros, revistas, imágenes y otros productos de la industria de la impresión (49)."""
    )

    bar1.bar_chart(top_valle_exports, x="POSAR", y="TOTAL_FOBPES")

    bar2.bar_chart(top_valle_exports_to_korea, x="POSAR", y="TOTAL_FOBPES")

    st.markdown(
        """
        Los anteriores gráficos evidencian una canasta exportadora vallecaucana
        desaprovechada en el comercio con Corea del Sur; con el agravante de tener un TLC vigente.
        
        La actual canasta exportadora hacia Corea del Sur, es de bajo volúmen comercial,
        y se basa en productor de poco valor agregado. Mientras que, en la exportaciones hacia el mundo,
        aparecen industrias como las coméstica y la farmacéutica, las cuales son de alto valor agregado."""
    )

    st.markdown(
        """
        ## Cluster de las Exportaciones Vallecaucanas"""
    )

    st.markdown(
        """
        ### Mundo
        
        El Valle del Cauca presenta una diversificación exportadora considerable.
        Se destacan los cluster de la industria textil, química, farmacéutica y de preparación de alimentos."""
    )
    st.altair_chart(cluster1)

    st.markdown(
        """
        ### Corea del Sur
        
        Sin embargo, el panorama en las exportaciones hacia Corea del Sur presenta
        un panorama totalmente opuesto con una concentración de las exportaciones
        en sectores de bajo valor agregado."""
    )
    st.altair_chart(cluster2)

    st.markdown(
        """
        ## Demanda Surcoreana"""
    )

    bar3, bar4 = st.columns(2)

    bar3.markdown(
        """
        ### Desde el Mundo"""
    )
    bar3.bar_chart(top_korea_imports, x="cmdCode", y="totalValue")

    bar4.markdown(
        """
        ### Desde Iberoamérica"""
    )
    bar4.bar_chart(top_korea_iberoamerica_imports, x="cmdCode", y="totalValue")

    st.markdown(
        """
        ### Desde Colombia"""
    )
    st.bar_chart(top_korea_colombia_imports, x="cmdCode", y="totalValue")

    st.markdown(
        """
        ## Cluster de Importación Surcoreano"""
    )

    cluster3 = (
        alt.Chart(cluster_korea_world_imports)
        .mark_circle()
        .encode(x="cmdCode", y="primaryValue", color="kmeans", tooltip=["cmdCode", "primaryValue", "kmeans"])
    )

    cluster4 = (
        alt.Chart(cluster_korea_colombia_imports)
        .mark_circle()
        .encode(x="cmdCode", y="primaryValue", color="kmeans", tooltip=["cmdCode", "primaryValue", "kmeans"])
    )

    tab1, tab2 = st.tabs(["Mundo", "Colombia"])

    tab1.altair_chart(cluster3)

    tab2.altair_chart(cluster4)



if __name__ == "__main__":
    main()