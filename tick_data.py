import pandas as pd
import os
from pandas import Series, DataFrame
from tqdm import tqdm

class TickDataAnalyze(object):

    def __init__(self, tick_file_prefix, end_date='00000000', end_time='10:00:00', multiple=2):
        self.tick_file_prefix = tick_file_prefix
        self.end_date = end_date
        self.end_time = end_time
        self.res_1 = []             # 不考虑 收盘或卖盘
        self.res_2 = []             # 只看 收盘
        self.res_3 = []             # 只看 卖盘
        self.multiple = multiple    # 前平均比全部平均大的倍数

    # 分析单只股票某天的数据
    def analyze_one_day(self, code, date):
        path = self.tick_file_prefix + str(code) + "\\" + date + ".csv"
        try:
            df = pd.read_csv(path)
        except:
            print("Error Open %s:%s" % (code, date))
        count = 0

        average_price_before = 0    # 截止时间 前 的均价
        average_price_after = 0     # 截止时间 后 的均价
        count_before = 0

        average_all = 0             # 全天成交手的平均值
        average_all_buy = 0         # 全天 买盘 成交手的平均值
        average_all_sold = 0        # 全天 卖盘 成交手的平均值

        average_before = 0          # 截止时间前的成交手平均值
        average_before_buy = 0      # 截止时间前的 买盘 成交手平均值
        average_before_sold = 0     # 截止时间前的 卖盘 成交手平均值
        
        for index, row in df.iterrows():
            if row['time'] <= self.end_time:
                count_before += 1
                average_price_before += row['price']
                average_before += row['volume']
                if row['type'] == "买盘":
                    average_before_buy += row['volume']
                else:
                    average_before_sold += row['volume']
            else:
                average_price_after += row['price']
            average_all += row['volume']
            if row['type'] == "买盘":
                average_all_buy += row['volume']
            else:
                average_all_sold += row['volume']
            count += 1

        if count_before == 0 or count == 0 or average_all == 0 or average_before == 0:
            # print("%s:%s数据不全"%(code, date))
            return

        # 求所有平均值
        average_price_before /= count_before
        average_price_after /= (count - count_before)

        average_all /= count
        average_all_buy /= count
        average_all_sold /= count

        average_before /= count_before
        average_before_buy /= count_before
        average_before_sold /= count_before
    
        # 检查是否符合条件
        dic = {'code': code, 'date': date, 'price_before': average_price_before, 'price_after': average_price_after}
        # 1. 不区分 买盘 或 卖盘
        if average_before / average_all > self.multiple:
            self.res_1.append(dic)
        # 2. 只看 买盘
        if average_all_buy == 0:
            return
        if average_before_buy / average_all_buy > self.multiple:
            self.res_2.append(dic)
        # 3. 只看 卖盘
        if average_all_sold == 0:
            return
        if average_before_sold / average_all_sold > self.multiple:
            self.res_3.append(dic)
    
    # 分析所有
    def analyze_all(self):
        file_list = os.listdir(self.tick_file_prefix)
        # print(file_list)
        for index in tqdm(range(len(file_list))):
            code = file_list[index]
            code_date_list = os.listdir(self.tick_file_prefix + code + "\\")
            for i in tqdm(range(len(code_date_list))):
                date = code_date_list[i]
                date = date[0:8]
                if date < self.end_date:
                    break
                self.analyze_one_day(code, date)
        print("******************************************************************************************************")
        print(self.res_1)
        print("******************************************************************************************************")
        print(self.res_2)
        print("******************************************************************************************************")
        print(self.res_3)
        print("******************************************************************************************************")

    def show_res(self, arr):
        txt = open('tick_data_res.txt',mode='w', encoding='utf-8')
        for index in arr:
            if index == 1:
                res = self.res_1
            elif index == 2:
                res = self.res_2
            elif index == 3:
                res = self.res_3
            else:
                continue
            print("\n\n\nres%s:" % index )
            txt.write("\n\n\nres%s:\n" % index )
            count = 0
            count_ = 0
            for i in res:
                count += 1
                code = i['code']
                temp = i['date']
                date = str(temp[0:4]) + "-" + str(temp[4:6]) + "-" + str(temp[-2:])
                time = self.end_time

                price_before = i['price_before']
                price_before = format(price_before, '.2f')
                price_before = " "*(6-len(price_before)) + str(price_before)

                price_after = i['price_after']
                price_after = format(price_after, '.2f')
                price_after = " "*(6-len(price_after)) + str(price_after)

                diff = i['price_after'] - i['price_before']
                if diff > 0:
                    count_ += 1
                diff = format(diff, ".2f")
                diff = " "*(6-len(diff)) + str(diff)

                tup = (code, date, time, price_before, time, price_after, diff)
                print("股票%s在 [%s] 满足条件， [%s] 前的均价为 %s , [%s] 后的均价为 %s , 相差%s" % tup)
                txt.write("股票%s在 [%s] 满足条件， [%s] 前的均价为 %s , [%s] 后的均价为 %s , 相差%s\n" % tup)
            rate = (count_ / count) * 100
            rate = format(rate, '.4f')
            rate += "%"
            print("共有%s种可能情况， 当天呈上升趋势的有%s种，占比%s" % (count, count_, rate))
            txt.write("共有%s种可能情况， 当天呈上升趋势的有%s种，占比%s\n" % (count, count_, rate))

    

if __name__ == '__main__':
    tick_file_prefix = "F:\\files\\sharesDatas\\tushare_tick_data\\"
    tick_data_analyze = TickDataAnalyze(tick_file_prefix)
    tick_data_analyze.analyze_one_day('600517', '20190415')
    # tick_data_analyze.analyze_all()
    tick_data_analyze.show_res([1,2,3])
