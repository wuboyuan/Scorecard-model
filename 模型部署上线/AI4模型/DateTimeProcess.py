
import pandas as pd


class DateTimeProcess:
    def __init__(self, s):
        # 格式化
        self.s = pd.to_datetime(s, errors='coerce')
        self.df = pd.DataFrame()
    
    def date_process(self):
        '''衍生日期特征
        return:
            isWeekend: 是否周末。0：否,1：是
            PeriodOfMonth: 1：上旬，2：中旬，3：下旬
        '''
        def _level(d):
            if d < (1 / 3.0):
                return 1
            elif d > (2 / 3.0):
                return 3
            else:
                return 2
#         self.df['Mth'] = self.s.dt.month
        t = self.s.dt.day * 1.0 / self.s.dt.daysinmonth
        self.df['PeriodOfMonth'] = t.apply(_level)
        # 数据在0~6之间，星期一是0，星期日是6
        self.df['isWeekend'] = (self.s.dt.dayofweek >= 5).apply(int)
    
    def time_process(self):
        '''衍生时间特征
        return:
        PeriodOfDay  0：深夜：1：上午；2：下午；3：晚上
        HourOfDay : 申请时间
        '''
        def _hour(hour):
            if (hour >= 0) & (hour <= 6):
                return 1
            elif (hour > 6) & (hour <= 12):
                return 2
            elif (hour > 12) & (hour <= 18):
                return 3
            else:
                return 4
        self.df['PeriodOfDay'] = self.s.dt.hour.apply(_hour)
        self.df['HourOfDay'] = self.s.dt.hour

    def process(self):
        self.date_process()
        self.time_process()
        return self.df