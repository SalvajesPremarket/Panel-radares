# ==========================================
# 🖥️ TABLA DE RESULTADOS (estilo Finviz oscuro)
# ==========================================
col_toggle, col_manual, col_info = st.columns([1.3, 1.3, 3])
with col_toggle:
    auto_on = st.toggle("Auto-refresh 15s", value=True, key="auto_refresh_toggle")
with col_manual:
    if st.button("🔄 Refrescar ahora"):
        st.rerun()
with col_info:
    st.markdown(f"**#1 / {len(ULTIMOS_RESULTADOS)} Total**")

if auto_on:
    st_autorefresh(interval=15000, key="auto_refresh_radar")

if ULTIMA_ACTUALIZACION:
    st.caption(f"Última actualización: {ULTIMA_ACTUALIZACION.strftime('%H:%M:%S')}")
else:
    st.caption("Esperando el primer escaneo con resultados...")

if ULTIMOS_RESULTADOS:
    df = pd.DataFrame([
        {
            "No.": i + 1,
            "Ticker": c["ticker"],
            "Precio": round(c["precio"], 2),
            "Cambio %": round(c["cambio_pct"], 1),
            "Volumen": formatear_numero_grande(c["volumen_dia"]),
            "Flotación": formatear_numero_grande(c.get("float_shares")),
            "Vol. Relativo": round(c.get("volumen_relativo", 0), 2),
            "Noticia": "🔥" if c.get("tiene_noticia") else "",
            "Actualizado": c["actualizado"].strftime("%H:%M:%S") if hasattr(c["actualizado"], "strftime") else c["actualizado"],
        }
        for i, c in enumerate(ULTIMOS_RESULTADOS)
    ])

    def color_cambio(val):
        try:
            v = float(val)
        except (TypeError, ValueError):
            return ''
        color = '#2ecc71' if v >= 0 else '#e74c3c'
        return f'color: {color}; font-weight: 700'

    styled = (
        df.style
        .map(color_cambio, subset=['Cambio %'])
        .set_properties(**{
            'background-color': '#12151c',
            'color': '#e6e6e6',
            'border-color': '#2a2e39'
        })
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#0e1117'), ('color', '#00ffcc'), ('font-weight', 'bold')]}
        ])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.info("Sin candidatos que cumplan los filtros en este momento.")
