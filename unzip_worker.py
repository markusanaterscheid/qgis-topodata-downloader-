"""
Módulo para descompactação e gerenciamento de arquivos ZIP com extração de TIFFs.

Copyright (C) 2025 Markus Scheid Anater
E-mail: markus.scheid.anater@gmail.com

Este programa é distribuído sob a licença GNU GPL v2 ou posterior.
"""

from PyQt5.QtCore import QObject, pyqtSignal
import os
import tempfile
import zipfile
import shutil

class UnzipWorker(QObject):
    started = pyqtSignal(str)
    progress = pyqtSignal(int, int)
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, zip_dir_path, zip_files):
        super().__init__()
        self.zip_dir_path = zip_dir_path
        self.unzip_dir_path = zip_dir_path
        self.zip_files = zip_files
        self.total_files = len(self.zip_files)
        self.current_index = 0
        self.unzip_files = []

    def start(self):
        """Inicia o processo de descompactação."""
        self.started.emit(f"Descompactando {self.total_files} arquivos ZIP...")
        self.unzip_next()

    def unzip_next(self):
        """Orquestra o processo de descompactação."""
        if self.current_index >= self.total_files:
            self.finished.emit(self.unzip_files)
            return

        zip_file_path = self.zip_files[self.current_index]
        
        try:
            temp_dir = self.create_temp_dir()
            self.extract_zip(zip_file_path, temp_dir)
            tif_files = self.find_tif_files(temp_dir)
            
            if tif_files:
                self.move_tif_files(tif_files)
            else:
                self.emit_tif_warning(zip_file_path)
                
            self.cleanup_temp_dir(temp_dir)
            
        except zipfile.BadZipFile:
            self.emit_corrupted_error(zip_file_path)
        except Exception as e:
            self.emit_generic_error(zip_file_path, e)
        finally:
            self.update_progress()

    def create_temp_dir(self):
        """Cria diretório temporário para extração."""
        temp_dir = tempfile.mkdtemp(dir=self.unzip_dir_path)
        return temp_dir

    def extract_zip(self, zip_path, extract_dir):
        """Descompacta arquivo ZIP para diretório especificado."""
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

    def find_tif_files(self, path):
        """
        Encontra todos os arquivos .tif no caminho especificado.
        Retorna lista de caminhos absolutos.
        """
        tif_files = []
        if os.path.isdir(path):
            for root, _, files in os.walk(path):
                for file in files:
                    if file.lower().endswith(".tif"):
                        full_path = os.path.join(root, file)
                        tif_files.append(full_path)
        return tif_files

    def move_tif_files(self, tif_files):
        """Move arquivos TIF para o diretório base."""
        for tif_file in tif_files:
            filename = os.path.basename(tif_file)
            dest_path = os.path.join(self.unzip_dir_path, filename)
            
            self.unzip_files.append(dest_path)

            # Evita sobrescrita de arquivos
            counter = 1
            while os.path.exists(dest_path):
                name, ext = os.path.splitext(filename)
                dest_path = os.path.join(
                    self.unzip_dir_path, 
                    f"{name}_{counter}{ext}"
                )
                counter += 1
                
            shutil.move(tif_file, dest_path)

    def cleanup_temp_dir(self, temp_dir):
        """Remove diretório temporário e seu conteúdo."""
        shutil.rmtree(temp_dir, ignore_errors=True)

    def emit_tif_warning(self, zip_path):
        """Notifica ausência de arquivos TIF."""
        err_msg = f"Aviso: Nenhum arquivo TIF encontrado em '{os.path.basename(zip_path)}'"
        self.error.emit(err_msg)

    def emit_corrupted_error(self, zip_path):
        """Notifica arquivo ZIP corrompido."""
        err_msg = f"Erro: O arquivo '{os.path.basename(zip_path)}' está corrompido."
        self.error.emit(err_msg)

    def emit_generic_error(self, zip_path, exception):
        """Notifica erro genérico durante descompactação."""
        err_msg = f"Erro inesperado ao descompactar '{os.path.basename(zip_path)}': {str(exception)}"
        self.error.emit(err_msg)

    def update_progress(self):
        """Atualiza progresso e avança para próximo item."""
        self.current_index += 1
        self.progress.emit(self.current_index, self.total_files)
        self.unzip_next()