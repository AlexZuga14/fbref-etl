import argparse
import pandas as pd

# Definir los mapeos y columnas a eliminar
COLUMNS_MAPPING = {
    "general": {"Gls. ": "goles", "Ass": "asist", "Dis": "tiros_totales", "DaP": "tiros_puerta",
                "ACT": "acciones_creacion_tiros", "ACG": "acciones_creacion_gol"},
    "pases": {"Cmp": "pases_completados", "% Cmp": "pases_completados_%", "Dist. tot.": "Distancia_total_del_pase",
              "Dist. prg.": "Distancia_pase_progresiva", "Cmp.1": "pases_completados_cortos",
              "Int..1": "pases_intentados_cortos", "% Cmp.1": "pases_completados_cortos_%",
              "Cmp.2": "pases_completados_medios", "Int..2": "pases_intentados_medios",
              "% Cmp.2": "pases_completados_medios_%", "Cmp.3": "pases_completados_largos",
              "Int..3": "pases_intentados_largos", "% Cmp.3": "pases_completados_largos_%",
              "Ass": "asistencias", "PC": "pases_clave", "1/3": "pases_ult_tercio",
              "PPA": "pases_en_area", "PrgP": "pases_progresivos"},
    "tipos_pase": {"Int.": "pases_intentados", "Camb.": "cambio_juego", "PA": "pases_fuera_juego",
                   "Bloqueos": "pases_bloqueados_opp"},
    "acciones_def": {"Tkl": "tackles", "TklG": "tackles_pos_recup", "Bloqueos": "bloqueos_balon",
                     "Dis": "disparos_bloqueados", "Pases": "pases_bloqueados", "Int": "intercepciones",
                     "Desp.": "despejes", "Err": "errors_provocan_tiro"},
    "posesion": {"Toques": "toques", "Def. pen.": "toques_area_penal_def", "3.º def.": "toques_tercio_def",
                 "3.º cent.": "toques_tercio_med", "3.º ataq.": "toques_tercio_ata", "Ataq. pen.": "toques_area_penal_ata",
                 "Dist. tot.": "distancia_conduccion(y)", "Dist. prg.": "distancia_conduccion_off(y)",
                 "Errores de control": "controles_fallidos", "Rec": "pases_recibidos",
                 "PrgR": "pases_progresivos_recibidos"},
    "diversos": {"2a amarilla": "2TA", "Fls": "faltas_cometidas", "FR": "faltas_recibidas", "PA": "fuera_juego",
                 "TklG": "tackle_con_recuperacion", "Penal ejecutado": "penal_ejecutado", "GC": "goles_propia",
                 'Recup.': "balones_sueltos_recup", "Ganados": "duelos_aereos_ganados",
                 "Perdidos": "duelos_aereos_perdidos"}
}

COLUMNS_DROP = {
    "general": ['TP', 'TPint', 'Cmp', 'Int', '% Cmp', 'PrgP', 'Transportes', 'PrgC', 'Att', 'Succ'],
    "pases": ['núm.', 'País', 'Posc', 'Edad', 'Mín', 'Int.', 'xAG', 'CrAP'],
    "tipos_pase": ['núm.', 'País', 'Posc', 'Edad', 'Mín', 'Balón vivo', 'Balón muerto', 'FK', 'PL', 'Pcz', 'Lanz.', 'SE', 'Dentro', 'Fuera', 'Rect.', 'Cmp'],
    "acciones_def": ['núm.', 'País', 'Posc', 'Edad', 'Mín', '3.º def.', '3.º cent.', '3.º ataq.', 'Att', 'Tkl%', 'Pérdida'],
    "posesion": ['núm.', 'País', 'Posc', 'Edad', 'Mín', 'Balón vivo', 'Att', 'Succ', 'Exitosa%', 'Tkld', 'Tkld%', 'Transportes', 'PrgC', '1/3', 'TAP', 'Des'],
    "diversos": ['núm.', 'País', 'Posc', 'Edad', 'Mín', 'TA', 'TR', 'Pcz', 'Int']
}

# Función de limpieza
def clean_fbref(df, col_names, col_drop):
    df.drop(labels=col_drop, axis=1, inplace=True, errors='ignore')
    df.drop(df.index[-1], axis=0, inplace=True)  # Elimina la última fila
    df.fillna(0, inplace=True)
    df.rename(columns=col_names, inplace=True)
    df.rename(columns=lambda x: x.replace(".", ""), inplace=True)
    df.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    try:
        df["País"] = df["País"].str.split(" ").str[1]
    except KeyError:
        pass
    try:
        df["Edad"] = df["Edad"].str.split("-").str[0]
    except KeyError:
        pass
    return df

# Función principal
def process_fbref_data(url, tipo="local"):
    # Definir los índices de las tablas según el tipo
    if tipo == "local":
        indices = [3, 4, 5, 6, 7, 8]
    elif tipo == "visitante":
        indices = [10, 11, 12, 13, 14, 15]
    else:
        raise ValueError("El parámetro 'tipo' debe ser 'local' o 'visitante'.")

    # Leer las tablas desde la URL
    tables = pd.read_html(url, header=1)

    # Procesar las tablas según los índices seleccionados
    df_gral = clean_fbref(tables[indices[0]], COLUMNS_MAPPING["general"], COLUMNS_DROP["general"])
    df_pases = clean_fbref(tables[indices[1]], COLUMNS_MAPPING["pases"], COLUMNS_DROP["pases"])
    df_tipo_pase = clean_fbref(tables[indices[2]], COLUMNS_MAPPING["tipos_pase"], COLUMNS_DROP["tipos_pase"])
    df_acciones_def = clean_fbref(tables[indices[3]], COLUMNS_MAPPING["acciones_def"], COLUMNS_DROP["acciones_def"])
    df_posesion = clean_fbref(tables[indices[4]], COLUMNS_MAPPING["posesion"], COLUMNS_DROP["posesion"])
    df_diversos = clean_fbref(tables[indices[5]], COLUMNS_MAPPING["diversos"], COLUMNS_DROP["diversos"])

    # Unir todas las tablas en un único DataFrame
    df_final = (df_gral.merge(df_pases, how='left', on='Jugador')
                .merge(df_tipo_pase, how='left', on='Jugador')
                .merge(df_acciones_def, how='left', on='Jugador')
                .merge(df_posesion, how='left', on='Jugador')
                .merge(df_diversos, how='left', on='Jugador'))

    # Generar el nombre del archivo basado en la URL y el tipo
    base_name = url.split("/")[-1]  # Toma la última parte de la URL
    file_name = f"{tipo}_{base_name}.csv"  # Añade el prefijo local/visitante

    # Guardar el archivo CSV
    df_final.to_csv(file_name, index=False)
    print(f"Archivo CSV generado: {file_name}")

    return df_final

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesa datos de FBRef y genera un CSV.")
    parser.add_argument("--url", required=True, type=str, help="URL de la página a procesar")
    parser.add_argument("--tipo", required=True, type=str, choices=["local", "visitante"], help="Tipo de datos a procesar: local o visitante")
    args = parser.parse_args()

    try:
        process_fbref_data(args.url, tipo=args.tipo)
    except Exception as e:
        print(f"Error al procesar los datos ({args.tipo}): {e}")
