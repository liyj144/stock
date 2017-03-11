#coding=utf-8

from DataManager import DataManager

if __name__ == '__main__':
    dm = DataManager()
    # dm.dropResult()
    # dm.createDb()
    # dm.deleteResult()
    # dm.manageDataCore(173889)
    # dm.computeArch(600000)
    dm.exportToXls()
    # dm.exportToXls(skip=350)
    # dm.test_query()