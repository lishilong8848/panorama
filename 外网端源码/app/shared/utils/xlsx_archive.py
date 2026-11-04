from zipfile import ZipFile
from xml.parsers.expat import ParserCreate

from openpyxl import load_workbook


def validate_xlsx_archive(source) -> None:
    with ZipFile(source) as archive:
        if len(set(archive.namelist())) != len(archive.infolist()):
            raise ValueError("工作簿压缩包有重复文件")
        for member in archive.infolist():
            with archive.open(member) as handle:
                if member.filename.lower().endswith((".xml", ".rels")):
                    ParserCreate().ParseFile(handle)
                else:
                    while handle.read(1024 * 1024):
                        pass
    workbook = load_workbook(source, read_only=True, data_only=True, keep_links=False)
    try:
        if not workbook.worksheets:
            raise ValueError("工作簿没有可读取的工作表")
    finally:
        workbook.close()
