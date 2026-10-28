DATASET = "linha_kml"

# As URLs dos KML vêm do silver de posições (não há endpoint que liste as linhas)
SOURCE_DATASET = "locate_bus"
SENTIDOS = {"LinhaKMLIda": "ida", "LinhaKMLVolta": "volta"}

# Pausa entre downloads: ~570 arquivos por execução, sem pressa
REQUEST_DELAY_SECONDS = 0.2

# Janela de dias do silver de posições usada para descobrir os KML: cobre linhas que não
# rodam todo dia (ex.: só no fim de semana)
DISCOVERY_DAYS = 7

# linha_kml_uso: sem posição da linha por mais que isso, o período de uso fecha
USO_GAP_MINUTES = 15
USO_DATASET = "linha_kml_uso"
