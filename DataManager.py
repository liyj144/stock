#coding=utf-8

from __future__ import print_function
from __future__ import print_function
from __future__ import division
import pandas as pd
import numpy as np
import logging
import logging.config
import os
import math
from arch import arch_model
from sqlalchemy import create_engine, Column, Integer, Float, String, and_, Index, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlite3
import pudb

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
    __table_args__ = (Index('stockfode', "date"),)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DataManager:
    '''
    股票数据处理
    '''
    engine = create_engine('sqlite:///stock.db', echo=False)
    conn = sqlite3.connect('stock.db')
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    excel_dir = '/wls/stock/xls/'
    once = 1000

    # 创建db
    def createDb(self):
        BaseModel.metadata.create_all(self.engine)

    #  drop result
    def dropResult(self):
        Result.__table__.drop(self.engine)

    # 清空result表
    def deleteResult(self):
        self.session.query(Result).delete()
        self.session.commit()

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
            except Exception as e:
                logger.error(e.message)

    '''
    处理数据 （结果保存至 result 表）
    start_pos: 起始位置
    with_check: 是否检查记录唯一性
    '''
    def managerData(self, start_pos=0, with_check=False):
        count = self.session.query(Trd).filter(Trd.id.__gt__(start_pos)).count()
        logger.info(count)
        loops = int(math.ceil(count / self.once))
        for i in xrange(loops):
            start = i * self.once
            end = (i + 1) * self.once - 1
            logger.info("Start to manage from " + str(start) + " to " + str(end))
            arTrd = self.session.query(Trd).filter(Trd.id.__gt__(start_pos)).offset(start).limit(self.once).all()
            # arTrd = self.session.query(Trd).filter(Trd.Stkcd == 600004).offset(start).limit(self.once).all()
            # arTrd = self.session.query(Trd).filter(and_(Trd.Stkcd == 600004, Trd.Trddt == '2012-01-04')).offset(start).limit(self.once).all()
            for trd in arTrd:
                if with_check:
                    check_trd = self.session.query(Result).filter(and_(Result.stockfode == trd.Stkcd, Result.date == trd.Trddt)).first()
                    if check_trd:
                        logger.info("check %d at %s already exist" % (trd.Stkcd, trd.Trddt))
                        continue
                idx = self.session.query(Idx).filter(Idx.Idxtrd01 == trd.Trddt).first()
                if not idx:
                    logger.error(trd.Trddt + ' is not ok')
                    continue
                o_result = self.computeData(trd, idx.Idxtrd08)
                try:
                    self.session.add(o_result)
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
                    logger.error("Error insert %s" % e.message)

    def manageDataCore(self, start_pos=0, with_check=False):
        count = self.session.query(Trd).filter(Trd.id.__gt__(start_pos)).count()
        logger.info(count)
        loops = int(math.ceil(count / self.once))
        for i in xrange(loops):
            start = i * self.once
            end = (i + 1) * self.once - 1
            logger.info("Start to manage from " + str(start) + " to " + str(end))
            arTrd = self.session.query(Trd).filter(Trd.id.__gt__(start_pos)).offset(start).limit(self.once).all()
            for trd in arTrd:
                # print(trd.Stkcd)
                # print(trd.Trddt)
                # return False
                if with_check:
                    check_trd = self.session.query(Result).filter(
                        and_(Result.stockfode == trd.Stkcd, Result.date == trd.Trddt)).first()
                    if check_trd:
                        logger.info("check %d at %s already exist" % (trd.Stkcd, trd.Trddt))
                        continue
                idx = self.session.query(Idx).filter(Idx.Idxtrd01 == trd.Trddt).first()
                if not idx:
                    logger.error(trd.Trddt + ' is not ok')
                    continue
                ar_result = self.computeData(trd, idx.Idxtrd08)
                row = ((ar_result.stockfode, ar_result.date, ar_result.T, ar_result.Mv, ar_result.Rm,
                       ar_result.Ri, ar_result.STDi, ar_result.STDm, ar_result.NRM, ar_result.YRM, ar_result.TM))
                cur = self.conn.cursor()
                cur.execute("INSERT INTO t_result (stockfode, date, T, Mv, Rm, Ri, STDi, STDm, NRM, YRM, TM) VALUES (?,?,?,?,?,?,?,?,?,?,?)", row)
            self.conn.commit()

    # 根据trd 和 idx 计算目标值
    def computeData(self, trd, idx):
        ar_result = Result()
        ar_result.stockfode = trd.Stkcd
        ar_result.date = trd.Trddt
        ar_result.T = trd.Dnvaltrd / (trd.Dsmvosd * 1000)
        ar_result.Mv = trd.Dsmvosd * 1000
        ar_result.Rm = idx / 100
        ar_result.Ri = trd.Dretnd - ar_result.Rm
        # 使用Dretnd 进行arch模型计算
        ar_result.STDi = 0
        # 使用Rm 记性arch模型计算
        ar_result.STDm = 0
        ar_result.NRM = ar_result.Rm if ar_result.Rm <= 0 else 0
        ar_result.YRM = ar_result.Rm if ar_result.Rm > 0 else 0
        ar_result.TM = abs(trd.Dretnd / ar_result.T)
        return ar_result

    def computeArch(self):
        ar_stock = self.session.query(distinct(Result.stockfode)).all()
        for index, stock in enumerate(ar_stock):
            stockfode = stock[0]
            logger.info("Start to compute %d stock, stock id is %d" % (index+1, stockfode))
            ar_result = self.session.query(Trd.id, Trd.Dretnd).filter(Trd.Stkcd == stockfode).all()
            logger.info(len(ar_result))
            if len(ar_result) > 4:
                # stdi
                ar_stdi = self.grach_with_array([result[1] for result in ar_result])
                str_stdi = ''.join([' when id = %d then %f ' % (ar_result[index][0], 0 if np.isnan(stdi) else stdi) for index, stdi in enumerate(ar_stdi)])
                # stdm
                ar_result = self.session.query(Result.id, Result.Rm).filter(Result.stockfode == stockfode).all()
                ar_stdm = self.grach_with_array([result[1] for result in ar_result])
                str_stdm = ''.join([' when id = %d then %f ' % (ar_result[index][0], 0 if np.isnan(stdm) else stdm) for index, stdm in enumerate(ar_stdm)])
                cur = self.conn.cursor()
                cur.execute("update t_result set STDi= CASE %s else STDi END, STDm= CASE %s else STDm END where stockfode = %d;" % (str_stdi, str_stdm, stockfode))
                self.conn.commit()
            else:
                logger.error("Error to arch stock %d , the array length is %d " % (stockfode, len(ar_result)))
    # 导出数据
    def grach_with_array(self, ar):
        #ar = [2.3, 2.5, 2.4, 1, 2, 3.2]
        # ar_date = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04', '2015-01-05', '2015-01-06']
        # am = arch_model(ar, mean='ARX', lags=2)
        #am = arch_model(ar, mean='ARX', lags=2, vol='Garch', p=1, o=0, q=1,power=2.0, dist='Normal', hold_back=None)
        am = arch_model(ar, mean='AR', lags=2, vol='Garch')
        return am.fit().conditional_volatility

    # 测试pandas 读取xls文件
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
        print(df.head())
        # print arch_model(df['Dretnd'], p=1, q=1).fit(update_freq=10).summary()
        print(arch_model(df['Dretnd'], p=1, q=1).fit())
        # write = ExcelWriter("output.xls")

