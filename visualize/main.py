import sys
import os
import streamlit as st
import duckdb
import pandas as pd

sys.path.append("/workspaces/talento_tech/ingest/")
import config  # type: ignore


def main():
    ddb = duckdb.connect("./data/transformed/db.duckdb")

    # Valle del Cauca datasets
    valle_exports = ddb.sql(
        """SELECT *
        FROM valle_exports;"""
    ).df()

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

    # Korea datasets
    korea_imports = ddb.sql(
        """SELECT *
        FROM korea_imports;"""
    ).df()

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

    # Close connection
    ddb.close()

    # Page Content
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
        - Libros, revistas, imágenes y otros productos de la industria de la impresión (49).
        """
    )

    bar1.bar_chart(
        top_valle_exports, x="POSAR", y="TOTAL_FOBPES", x_label="HS Code", y_label="COP"
    )

    bar2.bar_chart(
        top_valle_exports_to_korea,
        x="POSAR",
        y="TOTAL_FOBPES",
        x_label="HS Code",
        y_label="",
    )

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

    tab1, tab2 = st.tabs(["Mundo", "Corea del Sur"])

    tab1.markdown(
        """
        ### Mundo
        
        El Valle del Cauca presenta una diversificación exportadora considerable.
        Se destacan los cluster de la industria textil, química, farmacéutica, maquinaria y equipos eléctricos, y de preparación de alimentos."""
    )
    tab1.scatter_chart(
        cluster_valle_world_exports,
        x="POSAR",
        y="FOBPES",
        color="kmeans",
        x_label="HS Code",
        y_label="COP",
    )

    tab2.markdown(
        """
        ### Corea del Sur
        
        Sin embargo, el panorama en las exportaciones hacia Corea del Sur presenta
        un escenario totalmente opuesto con una concentración de las exportaciones
        en sectores de bajo valor agregado."""
    )
    tab2.scatter_chart(
        cluster_valle_korea_exports,
        x="POSAR",
        y="FOBPES",
        color="kmeans",
        x_label="HS Code",
        y_label="COP",
    )

    st.markdown(
        """
        ## Demanda Surcoreana"""
    )

    bar3, bar4 = st.columns(2)

    bar3.markdown(
        """
        ### Desde el Mundo"""
    )
    bar3.markdown(
        """
        Los principales sectores son:
        - Minerales, aceites y sustancias bituminosas (27).
        - Maquinaria y equipo eléctrico y sus partes (85).
        - Reactores nucleares y relacionados (85).
        - Vehículos, excepto material rodante ferroviario o de tranvía, y sus partes y accesorios (87).
        - Instrumentos y aparatos ópticos, médicos, quirúrjicos y sus partes (90).
        - Químicos inorgánicos (28).
        - Minerales, escoria y cenizas (26).
        - Hierro y acero (72).
        - Químicos orgánicos (29).
        - Plásticos y artículos de plástico (39)."""
    )
    bar3.bar_chart(
        top_korea_imports, x="cmdCode", y="totalValue", x_label="HS Code", y_label="USD"
    )

    bar4.markdown(
        """
        ### Desde Iberoamérica"""
    )
    bar4.markdown(
        """
        Los principales sectores son:
        - Minerales, escoria y cenizas (26).
        - Minerales, aceites y sustancias bituminosas (27).
        - Químicos inorgánicos (28).
        - Cereales (10).
        - Cobre y artículos de cobren (74).
        - Vehículos, excepto material rodante forroviaro o de tranvía, y sus partes y accesorios (87).
        - Residuos y artículos de la industria de alimentos (23).
        - Maquinaria y equípo eléctrico y sus partes (85).
        - Carne y despojos comestibles (02)."""
    )
    bar4.bar_chart(
        top_korea_iberoamerica_imports,
        x="cmdCode",
        y="totalValue",
        x_label="HS Code",
        y_label="",
    )

    st.markdown(
        """
        ### Desde Colombia"""
    )
    st.markdown(
        """
        Los principales sectores son:
        - Minerales, aceites y sustancias bituminosas (27).
        - Café, té, mate y especias (09).
        - Hierro y acero (72).
        - Cobre y artículos de cobre (74).
        - Árboles y otras plantas (06).
        - Preparaciones comestibles (21).
        - Productos químicos N.E.C (Not elsewhere Specified) (38).
        - Frutas y nueces comestibles (08).
        - Aluminio y artículos de aluminio (76)."""
    )
    st.bar_chart(
        top_korea_colombia_imports,
        x="cmdCode",
        y="totalValue",
        x_label="HS Code",
        y_label="USD",
    )

    st.markdown(
        """
        Corea del Sur se caracteriza por ser una economía de alto valor agregado.
        Esto puede explicar sus importaciones de insumos de bajo valor agregado
        para una posterior transformación.
        
        Sectores como los químicos inorgánicos, químicos orgánicos, plástico y artículos de plástico,
        instrumentos y apartos ópticos, médicos, quirúrjicos, y sus partes, y preparaciones comestibles,
        son tareas pendiente para Colombia, y más específicamente, el Valle del Cauca, qué,
        como se evidencia en los anteriores gráficos, tiene un cluster económico compatible
        con la demanda surcoreana."""
    )

    st.markdown(
        """
        ## Cluster de Importación Surcoreano"""
    )

    tab3, tab4 = st.tabs(["Mundo", "Colombia"])

    tab3.markdown(
        """
        Los clusters de importación surcoreana demuestran una economía compleja y diversificada,
        con grupos de foco en sectores de insumos para el sector industrial, como los contemplados
        desde el HS 20, hasta el HS 40; como también, en sectores de alta complejidad tecnológica
        como los que se contemplan desde el HS 80 en adelante."""
    )
    tab3.scatter_chart(
        cluster_korea_world_imports,
        x="cmdCode",
        y="primaryValue",
        color="kmeans",
        x_label="HS Code",
        y_label="USD",
    )

    tab4.markdown(
        """
        En general, la actividad comercial con Colombia es deficiente a pesar de la existencia de un TLC.
        Hay posibilidades de importación desde Colombia que no se están aprovechando."""
    )
    tab4.scatter_chart(
        cluster_korea_colombia_imports,
        x="cmdCode",
        y="primaryValue",
        color="kmeans",
        x_label="HS Code",
        y_label="USD",
    )

    st.markdown(
        """
        ## Conclusiones
        
        El propósito de los TLC's es la creación de comercio; es decir, aumentar las posibilidades de consumo por parte de cada una de las
        economías involucradas. Bajo la teoría de la integración económica, se plantea que la liberación comercial, aumenta la calidad de vida de las personas.
        Sin embargo, los TLC's, en países como Colombia, han sido incapaces de revelar sus efectos de mejora, y han sido inocuos, o en algunos casos, perjudiciales.
        Esto nos recuerda que el libre comercio, y la firma de tratados no garantiza el crecimiento económico, si no existen políticas e infraestructura adecuada
        para su aprovechamiento. Como se pudo ver en este informe, el Valle del Cauca tiene el potencial de ser una economía complementaria con la surcoreana;
        no obstante, la vigencia del TLC entre Colombia y Corea del Sur, no ha representado una modificación sustancial en la actividad económica entre las partes.
        El por qué de esta situación puede ser debido a varias circunstancias: (1) el desconocimiento de los ciudadanos sobre las oportunidades comerciales en el
        exterior, y las políticas de comercio exterior; (2) la inseguridad por el conflicto armado y el narcotráfico; (3) la falta de personas capacitadas en áreas
        de alto valor agregado; (4) la infraestructura deficiente que provoca un aumento en los costos logísticos. Este informe tuvo como objetivo solucionar la primer posible causa: el desconocimiento de los ciudadanos.
        
        El Valle del Cauca se caracteriza por un cluster económico fuerte en el área de la salud, cosméticos, belleza, químicos y fármacos; lo cual,
        lo posiciona como un potencial proveedor para las industrias surcoreanas, donde estos sectores, y especialmente, el cosmético y de belleza, tienen una
        gran relevancia. Es tarea de los emprendedores, empresarios, académicos, profesionales, y diseñadores de políticas públicas, echar un vistazo
        a las diferentes políticas de comercio exterior, para impulsar el crecimiento económico. Por ahora, Colombia firma tratados de libre comercio, pero sus ventajas comparativas en los diferentes departamentos están siendo desaprovechadas."""
    )

    st.markdown("## Explora los Datos")

    ddb = duckdb.connect("./data/transformed/db.duckdb")

    tab5, tab6 = st.tabs(["Valle del Cauca", "Corea del Sur"])

    expander = tab5.expander("Filtro")
    user = expander.text_area("¡Escribe una Query!", "SELECT * FROM valle_exports;")
    tab5.write(ddb.sql(user))

    expander = tab6.expander("Filtro")
    user = expander.text_area("¡Escribe una Query!", "SELECT * FROM korea_imports;")
    tab6.write(ddb.sql(user))

    ddb.close()


if __name__ == "__main__":
    main()
