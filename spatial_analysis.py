"""
Módulo de funções auxiliares para análise espacial e leitura de dados.

Copyright (C) 2025 Markus Scheid Anater
E-mail: markus.scheid.anater@gmail.com

Este programa é distribuído sob a licença GNU GPL v2 ou posterior.
"""

from qgis.core import (
    QgsVectorLayer,
    QgsGeometry,
    QgsCoordinateTransform,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsRasterLayer
)

from collections import defaultdict
from itertools import product
import os
import csv
import urllib.request
import zipfile
import tempfile

DIC_SUFFIX_DESC = {
    'ZN': 'altitude',
    'SN': 'declividade',
    'ON': 'orientacao',
    'RS': 'relevo',
    'FT': 'forma terreno',
    'DD': 'divisores',
    'VN': 'curvatura vertical',
    'HN': 'curvatura horizontal'
}

BASE_URL_TOPODATA = 'http://www.dsr.inpe.br/topodata/data/geotiff/'

def is_valid_polygon(polygon_path: str) -> bool:
    target_crs = QgsCoordinateReferenceSystem("EPSG:4674")

    # Carrega a camada.
    polygon_layer = QgsVectorLayer(polygon_path, "polygon", "ogr")
    
    return polygon_layer.isValid()


def is_polygon_within_grid(polygon_path: str, grid_path: str) -> bool:
    """
    Verifica se a camada de polígono está totalmente contida na camada de grade,
    reprojetando para SIRGAS 2000 (EPSG:4674) se necessário.

    Args:
        polygon_path (str): Caminho da camada de polígono a ser testada.
        grid_path (str): Caminho da camada de grade (polígono de referência).

    Returns:
        bool: True se o polígono estiver totalmente contido, False caso contrário.
    """
    # Define o CRS de destino como SIRGAS 2000 (EPSG:4674)
    target_crs = QgsCoordinateReferenceSystem("EPSG:4674")

    # 1. Carrega as camadas e valida se são válidas.
    polygon_layer = QgsVectorLayer(polygon_path, "polygon", "ogr")
    grid_layer = QgsVectorLayer(grid_path, "grid", "ogr")

    if not polygon_layer.isValid():
        raise ValueError(f"Camada de polígono inválida: {polygon_path}")
    if not grid_layer.isValid():
        raise ValueError(f"Camada de grade inválida: {grid_path}")
    
    # 2. Processa a camada de grade: reprojeta e combina as geometrias.
    grid_geom = QgsGeometry()
    grid_crs = grid_layer.crs()

    if grid_crs != target_crs:
        print(f"Reprojetando a camada de grade do CRS {grid_crs.authid()} para {target_crs.authid()}...")
        transform = QgsCoordinateTransform(grid_crs, target_crs, QgsProject.instance())
        
        for feat in grid_layer.getFeatures():
            geom = feat.geometry()
            if not geom.isNull():
                geom.transform(transform)
                if grid_geom.isNull():
                    grid_geom = geom
                else:
                    grid_geom = grid_geom.combine(geom)
    else:
        for feat in grid_layer.getFeatures():
            geom = feat.geometry()
            if not geom.isNull():
                if grid_geom.isNull():
                    grid_geom = geom
                else:
                    grid_geom = grid_geom.combine(geom)

    if grid_geom.isNull():
        return False
        
    # 3. Itera sobre cada polígono da camada de entrada: reprojeta e verifica a contenção.
    polygon_crs = polygon_layer.crs()
    
    if polygon_crs != target_crs:
        print(f"Reprojetando a camada de polígono do CRS {polygon_crs.authid()} para {target_crs.authid()}...")
        transform = QgsCoordinateTransform(polygon_crs, target_crs, QgsProject.instance())
        
        for feat in polygon_layer.getFeatures():
            polygon_geom = feat.geometry()
            if not polygon_geom.isNull():
                polygon_geom.transform(transform)
                if not polygon_geom.within(grid_geom):
                    return False
    else:
        for feat in polygon_layer.getFeatures():
            polygon_geom = feat.geometry()
            if not polygon_geom.isNull() and not polygon_geom.within(grid_geom):
                return False

    # Se todas as feições foram verificadas e estão contidas, retorna True.
    return True



def analyze_polygon_against_grid(polygon_path, grid_path):
    """
    Analisa interseções entre um polígono (GeoJSON ou Shapefile) e uma grade vetorial.
    
    Args:
        polygon_path (str): Caminho para o arquivo de polígono (GeoJSON ou Shapefile)
        grid_path (str): Caminho para o arquivo da grade (GeoJSON ou Shapefile)
    
    Returns:
        list: Lista ordenada de códigos únicos de grade que intersectam ou contém o polígono.
    """
    # Carregar camadas vetoriais com o provedor OGR
    poly_layer = QgsVectorLayer(polygon_path, "PolygonLayer", "ogr")
    grid_layer = QgsVectorLayer(grid_path, "GridLayer", "ogr")

    if not poly_layer.isValid():
        raise Exception(f"Camada de polígono inválida: {polygon_path}")
    if not grid_layer.isValid():
        raise Exception(f"Camada de grade inválida: {grid_path}")

    # Verifica e aplica transformação de CRS se necessário
    if poly_layer.crs() != grid_layer.crs():
        transform = QgsCoordinateTransform(poly_layer.crs(), grid_layer.crs(), QgsProject.instance())
    else:
        transform = None

    results = []

    for poly_feat in poly_layer.getFeatures():
        poly_geom = poly_feat.geometry()
        if not poly_geom or poly_geom.isEmpty():
            continue

        if transform:
            poly_geom.transform(transform)

        for grid_feat in grid_layer.getFeatures():
            grid_geom = grid_feat.geometry()
            if not grid_geom or grid_geom.isEmpty():
                continue

            try:
                tile_code = grid_feat["tile_code"]
            except KeyError:
                raise Exception("Campo 'tile_code' não encontrado na camada da grade")

            if poly_geom.within(grid_geom):
                results.append({
                    "polygon_index": poly_feat.id(),
                    "tile_code": tile_code,
                    "relation": "within"
                })
            elif poly_geom.intersects(grid_geom):
                results.append({
                    "polygon_index": poly_feat.id(),
                    "tile_code": tile_code,
                    "relation": "intersects"
                })

    # Retorna lista única e ordenada de códigos
    unique_tile_codes = sorted(set(r["tile_code"] for r in results))
    return unique_tile_codes


def create_quadrant_dict(data: list[dict]) -> dict:
    """
    Transforms a list of data dictionaries into a dictionary keyed by quadrant code.
    Each key holds a list of dictionaries for all files belonging to that quadrant.

    Args:
        data: A list of dictionaries (from the CSV file).

    Returns:
        A dictionary where keys are 'code' and values are lists
        of dictionaries with the corresponding file information.
    """
    # Using defaultdict simplifies grouping items by a key
    quadrants = defaultdict(list)
    
    for row in data:
        # Get the quadrant code from the current row
        quadrant_code = row.get('code')
        
        # If the code exists, append the entire row to the corresponding list
        if quadrant_code:
            quadrants[quadrant_code].append(row)
            
    # Convert defaultdict back to a regular dict
    return dict(quadrants)

def read_csv_file(filename: str) -> list[dict]:
    """
    Reads a CSV file and returns the data as a list of dictionaries.

    Args:
        filename: The path to the CSV file.

    Returns:
        A list of dictionaries, where each dictionary represents a row.
        Returns an empty list if the file is not found or an error occurs.
    """
    data = []
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            # The DictReader reads each row as a dictionary using the header as keys.
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data.append(row)
        print(f"File '{filename}' read successfully.")
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    return data


def find_missing_from_quadrant_dict(quadrant_dict: dict, codes: list[str], suffixes: list[str]) -> list[tuple[str, str]]:
    # gerar todas as combinações esperadas
    expected = set(product(codes, suffixes))

    # gerar todas as combinações existentes no dicionário
    existing = set(
        (code, item["suffix"])
        for code, items in quadrant_dict.items()
        for item in items
    )

    # retornar o que está no esperado, mas não no existente
    missing = expected - existing
    return list(missing)

def filter_quadrant_by_suffix(suffixes: list[str], unique_tile_codes: list[str], quadrant_dict: dict)-> dict:
    
    quadrant_dict_filtered = {}
    for code in unique_tile_codes:
        quadrant_dict_filter = filter_values_by_suffix(suffixes, quadrant_dict[code])
        quadrant_dict_filtered[code] = quadrant_dict_filter
    return quadrant_dict_filtered
        

def filter_values_by_suffix(suffixes: list[str], data: list[dict]) -> list[dict]:
    return [item for item in data if item.get('suffix') in suffixes]


def load_url_quadrants(quadrant_dict: dict)-> list[str]:
    url = []
    for quadrant_list in quadrant_dict.values():
        for quadrant in quadrant_list:
            url.append(f'{BASE_URL_TOPODATA}{quadrant['file_name']}')
    return url


def find_tif(dir):
    tifs = []
    for root, _, files in os.walk(dir):
        for file in files:
            if file.lower().endswith('.tif') or file.lower().endswith('.tiff'):
                tifs.append(os.path.join(root, file))
    return tifs


def add_layers(tifs):
    """
    Adiciona camadas raster e retorna o status de cada uma.
    """
    for tif_path in tifs:
        layer = QgsRasterLayer(tif_path, os.path.basename(tif_path))
        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
            yield (tif_path, True)  # Sucesso
        else:
            yield (tif_path, False) # Falha

def test_polygon_grid_intersection(polygon_path, grid_path):
    poly_layer = QgsVectorLayer(polygon_path, "PolygonLayer", "ogr")
    grid_layer = QgsVectorLayer(grid_path, "GridLayer", "ogr")

    if not poly_layer.isValid():
        print("Camada polígono inválida")
        return
    if not grid_layer.isValid():
        print("Camada grade inválida")
        return

    # Ajusta CRS
    if poly_layer.crs() != grid_layer.crs():
        transform = QgsCoordinateTransform(poly_layer.crs(), grid_layer.crs(), QgsProject.instance())
    else:
        transform = None

    for poly_feat in poly_layer.getFeatures():
        poly_geom = poly_feat.geometry()
        if not poly_geom or poly_geom.isEmpty():
            print(f"Polígono {poly_feat.id()} vazio ou inválido")
            continue

        if transform:
            poly_geom = poly_geom.clone()
            poly_geom.transform(transform)

        found_tiles = []

        for grid_feat in grid_layer.getFeatures():
            grid_geom = grid_feat.geometry()
            if not grid_geom or grid_geom.isEmpty():
                continue

            tile_code = grid_feat["tile_code"] if "tile_code" in grid_feat.fields().names() else None
            if tile_code is None:
                print("Campo 'tile_code' não encontrado")
                return

            # Testa contenção e interseção
            if poly_geom.within(grid_geom):
                print(f"Polígono {poly_feat.id()} está DENTRO do tile {tile_code}")
                found_tiles.append(tile_code)
            elif poly_geom.intersects(grid_geom):
                print(f"Polígono {poly_feat.id()} INTERSECTA tile {tile_code}")
                found_tiles.append(tile_code)

        if not found_tiles:
            print(f"Nenhum tile encontrado para polígono {poly_feat.id()}")
        else:
            unique_tiles = sorted(set(found_tiles))
            print(f"Tiles encontrados para polígono {poly_feat.id()}: {unique_tiles}")


