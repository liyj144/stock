#coding=utf-8

from __future__ import division
import pandas as pd
import logging
import logging.config
import os
import math
from arch import arch_model
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, Float, Date, String, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('main')

BaseModel = declarative_base()


class Trd(BaseModel):
    __tablename__ = 't_trd'
    id = Column(Integer, primary_key=True, autoincrement=True)
    Stkcd = Column(Integer)
    Trddt = Column(String(32))
    Dnvaltrd = Column(Float)
    Dsmvosd = Column(Float)
    Dretnd = Column(Float)

class Idx(BaseModel):
    __tablename__ = 't_idx'
    id = Column(Integer, primary_key=True, autoincrement=True)
    Idxtrd01 = Column(String(32))
    Idxtrd08 = Column(Float)


class Result(BaseModel):
    __tablename__ = 't_result'
    id = Column(Integer, primary_key=True, autoincrement=True)
    stockfode = Column(Integer)
    date = Column(String(32))
    T = Column(Float)
    Mv = Column(Float)
    Rm = Column(Float)
    Ri = Column(Float)
    STDi = Column(Float)
    STDm = Column(Float)
    NRM = Column(Float)
    YRM = Column(Float)
    TM = Column(Float)



class DataManager:
    '''
    股票数据处理
    '''
    engine = create_engine('sqlite:///stock.db', echo=True)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    excel_dir = '/wls/stock/xls/'
    once = 5

    # 创建db
    def createDb(self):
        metadata = MetaData(self.engine)
        logger.info("Start to create table t_trd")
        t_trd = Table('t_trd', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('stkcd', Integer, index=True),
                      Column('trddt', Date),
                      Column('dnvaltrd', Float),
                      Column('dsmvosd', Float),
                      Column('dretnd', Float),
                      )
        t_trd.create()
        logger.info("Create table t_trd ok ... ")
        logger.info("Start to create table t_idx")
        t_idx = Table('t_idx', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('idxtrd01', Date),
                      Column('idxtrd08', Float),
                      )
        t_idx.create()
        logger.info("Create table t_idx ok ... ")
        logger.info("Start to create table t_result")
        t_result = Table('t_result', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('stkcd', Integer, index=True),
                      Column('trddt', Date),
                      Column('T', Float),
                      Column('MV', Float),
                      Column('Rm', Float),
                      Column('Ri', Float),
                      Column('STDi', Float),
                      Column('STDm', Float),
                      Column('NRm', Float),
                      Column('YRm', Float),
                      Column('Tm', Float),
                      )
        t_result.create()
        logger.info("Create table t_result ok ... ")
        logger.info("Create all table success !")

    # 导入trd、idx基础数据到sqlite数据库(trd 表)
    def importData(self):
        arFile = os.listdir(self.excel_dir)
        counter = 0
        for file in arFile:
            if file[0] == '.':
                continue
            counter += 1
            logger.info("Start to import " + str(counter) + " file: " + file)
            try:
                df = pd.read_excel(self.excel_dir + file)
                if file[0:3] == 'TRD':
                    df.to_sql('t_trd', self.engine, index=False, if_exists='append')
                elif file[0:3] == 'IDX':
                    df.to_sql('t_idx', self.engine, index=False, if_exists='append')
            except Exception, e:
                logger.error(e.message)


    # 处理数据 （结果保存至 result 表）
    def managerData(self):
        count = self.session.query(Trd).count()
        logger.info(count)
        loops = int(math.ceil(count / self.once))
        for i in xrange(loops):
            if i >= 2:
                break
            start = i * self.once
            end = (i + 1) * self.once - 1
            logger.info("Start to manage from " + str(start) + " to " + str(end))
            #arTrd = self.session.query(Trd).filter(Trd.Stkcd == 600004).offset(start).limit(self.once).all()
            arTrd = self.session.query(Trd).filter(and_(Trd.Stkcd == 600004, Trd.Trddt == '2012-01-04')).offset(start).limit(self.once).all()
            for trd in arTrd:
                idx = self.session.query(Idx).filter(Idx.Idxtrd01 == trd.Trddt).first()
                if not idx:
                    logger.error(trd.Trddt + ' is not ok')
                    continue
                ar_result = self.computeData(trd, idx.Idxtrd08)
                print ar_result

    # 根据trd 和 idx 计算目标值
    def computeData(self, trd, idx):
        print trd, idx
        ar_result = {}
        ar_result['stockfode'] = trd.Stkcd
        ar_result['date'] = trd.Trddt
        ar_result['T'] = trd.Dnvaltrd / (trd.Dsmvosd * 1000)
        ar_result['MV'] = trd.Dsmvosd * 1000
        ar_result['Rm'] = idx / 100
        ar_result['Ri'] = trd.Dretnd - ar_result['Rm']
        # 使用Dretnd 进行arch模型计算
        ar_result['STDi'] = 0
        # 使用Rm 记性arch模型计算
        ar_result['STDm'] = 0
        ar_result['NRm'] = ar_result['Rm'] if ar_result['Rm'] <= 0 else 0
        ar_result['YRm'] = ar_result['Rm'] if ar_result['Rm'] > 0 else 0
        ar_result['TM'] = abs(trd.Dretnd / ar_result['T'])
        return ar_result

    # 导出数据
    def exportData(self):
        ar = [2.3, 4.5, 2.4, 1,2, 3.2]
        print 123


    def printXls(self):
        df = pd.read_excel("TRD_Dalyr.xls")
        df_idx = pd.read_excel("IDX_Idxtrd.xls")
        # df = df.convert_objects(convert_numeric=True)
        df['T'] = df['Dnvaltrd'] / df['Dsmvosd']
        df['MV'] = df['Dsmvosd']
        # 指数回报率
        df['Rm'] = df_idx['Idxtrd08'] / 100
        df['Ri'] = df['Dretnd'] - df['Rm']
        # df['STDi'] = arch.arch_model(df['Dretnd'])
        # df['STDm'] = arch.arch_model(df['Rm'])
        # df['NRm'] = df['Rm'] if df['Rm'] <= 0 else 0
        # df['YRm'] = df['Rm'] if df['Rm'] > 0 else 0
        df['TM'] = df['Dsmvosd'] / df['T']
        print df.head()
        # print arch_model(df['Dretnd'], p=1, q=1).fit(update_freq=10).summary()
        print arch_model(df['Dretnd'], p=1, q=1).fit()
        # write = ExcelWriter("output.xls")

