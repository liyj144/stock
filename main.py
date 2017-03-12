#coding=utf-8

from DataManager import DataManager

if __name__ == '__main__':
    dm = DataManager()
    # dm.dropResult()
    # dm.createDb()
    # dm.deleteResult()
    # dm.manageDataCore(173889)
    # dm.computeArch()
    # dm.exportToXls(skip=0, limit=3)
    # dm.exportToXls()
    dm.exportToXls(skip=350)
    # dm.test_query()