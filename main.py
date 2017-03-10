#coding=utf-8

from DataManager import DataManager

if __name__ == '__main__':
    dm = DataManager()
    # dm.dropResult()
    dm.createDb()
    dm.deleteResult()
    dm.managerData()

