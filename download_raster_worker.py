"""
Módulo que implementa um worker para baixar múltiplos arquivos via rede sequencialmente,
emitindo sinais para monitoramento de progresso, conclusão e erros.

Copyright (C) 2025 Markus Scheid Anater
E-mail: markus.scheid.anater@gmail.com

Este programa é distribuído sob a licença GNU GPL v2 ou posterior.
"""

from qgis.PyQt.QtCore import QObject, QUrl, pyqtSignal, QTimer
from qgis.PyQt.QtNetwork import QNetworkAccessManager, QNetworkRequest, QNetworkReply
import os

class DownloadWorker(QObject):
    started = pyqtSignal(str)
    file_started = pyqtSignal(str, int)
    download_progress = pyqtSignal(int, int)
    file_finished = pyqtSignal()
    finished = pyqtSignal(str, list)
    error = pyqtSignal(str)

    def __init__(self, urls, dest_path):
        super().__init__()
        self.urls = urls
        self.dest_path = dest_path
        self.nam = QNetworkAccessManager()
        self.current_index = 0
        self.reply = None
        self.total_urls = len(urls)
        self.zip_files_downloaded = []
        self._is_canceled = False

    def start(self):
        self.started.emit(f"Iniciando download de {self.total_urls} arquivos...")
        QTimer.singleShot(0, self.download_next)

    def cancel(self):
        self._is_canceled = True
        if self.reply is not None:
            self.reply.abort()  # cancela o download ativo

    def download_next(self):

        if self._is_canceled:
            self.finished.emit(self.dest_path, self.zip_files_downloaded)
            return
        if self.current_index >= self.total_urls:
            self.finished.emit(self.dest_path, self.zip_files_downloaded)
            return

        if self.current_index >= self.total_urls:
            self.started.emit("Todos os downloads foram concluídos.")
            self.finished.emit(self.dest_path, self.zip_files_downloaded)
            return

        url = self.urls[self.current_index]
        file_name = os.path.basename(QUrl(url).path())
        self.file_started.emit(file_name, self.current_index)

        request = QNetworkRequest(QUrl(url))
        self.reply = self.nam.get(request)

        self.reply.downloadProgress.connect(self.handle_download_progress)
        self.reply.finished.connect(self.handle_finished)
        self.reply.errorOccurred.connect(self.handle_error)
        self.reply.sslErrors.connect(lambda errors: self.error.emit(f"Erro SSL: {errors}"))

    def handle_download_progress(self, bytes_received, bytes_total):
        if self._is_canceled:
            if self.reply is not None:
                self.reply.abort()
            return
        if bytes_total > 0:
            self.download_progress.emit(bytes_received, bytes_total)

    def handle_finished(self):
        if self.reply.error() != QNetworkReply.NoError:
            # O erro já foi tratado em handle_error
            self.cleanup_and_continue()
            return

        try:
            data = self.reply.readAll()
            zip_name = os.path.basename(self.urls[self.current_index])
            zip_path = os.path.join(self.dest_path, zip_name)

            os.makedirs(os.path.dirname(zip_path), exist_ok=True)

            with open(zip_path, 'wb') as f:
                f.write(data.data())  # usa .data() para evitar problemas com QByteArray

            self.zip_files_downloaded.append(zip_path)
            self.started.emit(f"Arquivo salvo com sucesso: {zip_name}")
        except Exception as e:
            self.error.emit(f"Erro ao salvar o arquivo {zip_name}: {e}")
        
        self.cleanup_and_continue()

    def handle_error(self, code):
        if self._is_canceled:
            # Pode emitir uma mensagem específica ou ignorar
            self.finished.emit(self.dest_path, self.zip_files_downloaded)
            return
        url = self.urls[self.current_index]
        err_msg = f"Erro ao baixar {url} ({self.reply.errorString()})"
        self.error.emit(err_msg)
        self.cleanup_and_continue()

    def cleanup_and_continue(self):
        if self.reply:
            self.reply.deleteLater()
            self.reply = None
        self.file_finished.emit()
        self.current_index += 1
        QTimer.singleShot(0, self.download_next)
