import json
import time
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import lark_oapi as lark
from lark_oapi.api.drive.v1.model.create_export_task_request import (
    CreateExportTaskRequest,
)
from lark_oapi.api.drive.v1.model.create_export_task_response import (
    CreateExportTaskResponse,
)
from lark_oapi.api.drive.v1.model.download_export_task_request import (
    DownloadExportTaskRequest,
)
from lark_oapi.api.drive.v1.model.download_export_task_response import (
    DownloadExportTaskResponse,
)
from lark_oapi.api.drive.v1.model.export_task import ExportTask
from lark_oapi.api.drive.v1.model.get_export_task_request import GetExportTaskRequest
from lark_oapi.api.drive.v1.model.get_export_task_response import GetExportTaskResponse

from xrpa_core.core.logger import logger


class FeishuExportTask:
    def __init__(self, token: str, ticket: str):
        self.token = token
        self.ticket = ticket
        self.file_token = None
        self.job_status = None


class FeishuDocType(StrEnum):
    DOC = "doc"  # 旧版飞书文档
    SHEET = "sheet"  # 飞书电子表格
    BITABLE = "bitable"  # 飞书多维表格
    DOCX = "docx"  # 新版飞书文档


class FeishuFileExtension(StrEnum):
    DOCX = "docx"  # Microsoft Word
    PDF = "pdf"  # PDF
    XLSX = "xlsx"  # Microsoft Excel
    CSV = "csv"  # CSV


class FeishuDocExporter:
    def __init__(self, client):
        self.client = client

    def create_export_task(
        self,
        token: str,
        file_extension: FeishuFileExtension,
        doc_type: FeishuDocType,
        sub_id: str | None = None,
    ) -> FeishuExportTask:
        """
        create_export_task 的 Docstring

        :param self: 说明
        :param token: 说明
        :type token: str
        :param file_extension: 说明
        :type file_extension: FeishuFileExtension
        :param doc_type: 说明
        :type doc_type: FeishuDocType
        :param sub_id: 说明
        :type sub_id: str
        """
        builder = (
            ExportTask.builder()
            .file_extension(file_extension)
            .token(token)
            .type(doc_type)
        )
        if sub_id:
            builder = builder.sub_id(sub_id)

        request: CreateExportTaskRequest = (
            CreateExportTaskRequest.builder().request_body(builder.build()).build()
        )

        response: CreateExportTaskResponse = self.client.drive.v1.export_task.create(
            request
        )

        if not response.success():
            raise Exception(
                f"client.drive.v1.export_task.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
            )

        logger.debug(response.data)
        return FeishuExportTask(token=token, ticket=response.data.ticket)

    def update_task_status(self, task: FeishuExportTask):
        """
        query_status 的 Docstring

        :param self: 说明
        :param task: 说明
        :type task: FeishuExportTask
        """
        request: GetExportTaskRequest = (
            GetExportTaskRequest.builder().ticket(task.ticket).token(task.token).build()
        )
        response: GetExportTaskResponse = self.client.drive.v1.export_task.get(request)

        if not response.success():
            lark.logger.error(
                f"client.drive.v1.export_task.get failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
            )
            return None

        lark.logger.debug(lark.JSON.marshal(response.data, indent=4))

        task.job_status = response.data.result.job_status
        if response.data.result.job_status == 0:
            task.file_token = response.data.result.file_token
        return response.data.result.job_status

    def download_task(self, save_dir: str, task: FeishuExportTask):
        save_path = Path(save_dir)

        request: DownloadExportTaskRequest = (
            DownloadExportTaskRequest.builder().file_token(task.file_token).build()
        )

        response: DownloadExportTaskResponse = (
            self.client.drive.v1.export_task.download(request)
        )

        if not response.success():
            lark.logger.error(
                f"client.drive.v1.export_task.download failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
            )
            return None

        name = response.file_name
        if not name:
            raise RuntimeError("下载失败，response中没有文件名")

        stem, suffix = name.rsplit(".", 1)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        new_name = f"{stem}_{timestamp}.{suffix}"
        file_path = save_path.joinpath(new_name)

        if not response.file:
            raise RuntimeError("下载失败，response中没有文件内容")

        with open(str(file_path), "wb") as f:
            f.write(response.file.read())
        return file_path

    def export_and_download(
        self,
        token: str,
        file_extension: FeishuFileExtension,
        doc_type: FeishuDocType,
        save_dir: str,
        sub_id: str | None = None,
        max_wait_time: int = 300,
        poll_interval: int = 3,
    ) -> Path | None:
        """
        导出并下载飞书文档的完整流程

        :param token: 文档token
        :type token: str
        :param file_extension: 导出文件格式
        :type file_extension: FeishuFileExtension
        :param doc_type: 文档类型
        :type doc_type: FeishuDocType
        :param save_dir: 保存目录
        :type save_dir: str
        :param sub_id: 子文档ID（可选）
        :type sub_id: str
        :param max_wait_time: 最大等待时间（秒），默认300秒
        :type max_wait_time: int
        :param poll_interval: 轮询间隔（秒），默认3秒
        :type poll_interval: int
        :return: 下载文件的路径，失败返回None
        :rtype: Path | None
        """
        # 创建导出任务
        task = self.create_export_task(token, file_extension, doc_type, sub_id)
        if not task:
            logger.error("创建导出任务失败")
            return None

        # 轮询任务状态
        start_time = time.time()
        while True:
            # 检查是否超时
            if time.time() - start_time > max_wait_time:
                logger.error(f"导出任务超时，已等待 {max_wait_time} 秒")
                return None

            # 更新任务状态
            status = self.update_task_status(task)

            if status == 0:
                # 导出完成，开始下载
                logger.info("导出完成，开始下载")
                file_path = self.download_task(save_dir, task)
                if file_path:
                    logger.info(f"下载完成: {file_path}")
                    return file_path
                else:
                    logger.error("下载失败")
                    return None

            elif status == 1:
                logger.debug("任务初始化中")

            elif status == 2:
                logger.debug("任务处理中")

            elif status == 3:
                logger.error("内部错误")
                return None

            elif status == 107:
                logger.error("导出文档过大")
                return None

            else:
                logger.error(f"导出失败，状态码: {status}")
                return None

            # 等待后继续轮询
            time.sleep(poll_interval)
