#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import joblib
import pandas as pd
import numpy as np
import time
from parse_data import Request_data
#from update_app_info import update_info
import warnings

warnings.filterwarnings("ignore")


import joblib
import pandas as pd
import numpy as np
import time
from parse_data import Request_data
#from update_app_info import update_info
import warnings

warnings.filterwarnings("ignore")


class AddFeatures:
    def __init__(self, parse_data=None, gp_data=None):
        self.gp_data = gp_data
        self.parse_data = parse_data

    def write_mapping_app_info(self, gp_data=None):
        class_category = {
            "模拟": "game",
            "益智": "game",
            "街机": "game",
            "冒险": "game",
            "竞速": "game",
            "文字": "game",
            "休闲": "game",
            "动作": "game",
            "体育": "game",
            "角色扮演": "game",
            "策略": "game",
            "桌面和棋类": "gamble",
            "卡牌": "gamble",
            "赌场": "gamble",
            "漫画": "entertainment",
            "娱乐 ": "entertainment",
            "音乐与音频": "video_music",
            "视频播放和编辑": "video_music",
            "音乐": "video_music",
            "工具": "tool",
            "财务": "finance",
            "教育": "education",
            "育儿": "education",
            "知识问答": "education",
            "图书与工具书": "ebook",
            "摄影": "lifestyle",
            "生活时尚": "lifestyle",
            "美容时尚": "lifestyle",
            "餐饮美食": "lifestyle",
            "医疗": "lifestyle",
            "天气": "lifestyle",
            "家居装修": "lifestyle",
            "活动": "lifestyle",
            "旅游与本地出行": "travel",
            "健康与健身": "travel",
            "购物": "travel",
            "地图和导航": "travel",
            "车辆和交通": "travel",
            "社交": "social",
            "社交约会": "social",
            "新闻杂志": "social",
            "艺术和设计": "tool",
            "软件库与演示": "tool",
            "通讯": "communication",
            "商务办公": "business",
            "个性化": "personal",
            "公司": "company"
        }
        gp_data["class_category"] = gp_data["category"].map(class_category).fillna("others")
        gp_class_category = gp_data["class_category"].tolist()
        """冷热门app程度写入文件"""
        popular_app_dict = {}
        # 评论低于500的app安装包名字
        review_unpopular_app = gp_data[
            gp_data.review_total < 500]["package_name"].tolist()
        # 评论过500-10000的app安装包名字
        review_unpopular_second_app = gp_data[(gp_data.review_total >= 500) & (
                gp_data.review_total < 10000)]["package_name"].tolist()
        # 评论过1万-10万的app安装包名字
        review_middle_app = gp_data[(gp_data.review_total >= 10000) & (
                gp_data.review_total <= 100000)]["package_name"].tolist()
        # 评论过10万的app安装包名字
        review_popular_app = gp_data[
            gp_data.review_total > 100000]["package_name"].tolist()
        # 安装低于5000的app安装包名字
        install_unpopular_app = gp_data[
            gp_data.install_total < 5000]["package_name"].tolist()
        # 安装低于5000-30000的app安装包名字
        install_unpopular_second_app = gp_data[(gp_data.install_total >= 5000) & (
                gp_data.install_total < 100000)]["package_name"].tolist()
        # 安装低于10w-100w的app安装包名字
        install_middle_app = gp_data[(gp_data.install_total >= 100000) & (
                gp_data.install_total < 1000000)]["package_name"].tolist()
        # 安装过100万的app安装包名字
        install_popular_app = gp_data[
            gp_data.install_total > 1000000]["package_name"].tolist()
        # 贷款安装低于1w-的app安装包名字
        loan_app_data = gp_data[gp_data.category == "finance"]
        cash_install_unpopular_app = loan_app_data[(
                loan_app_data.install_total < 10000)]["package_name"].tolist()
        # 贷款安装低于1w-10w的app安装包名字
        loan_app_data = gp_data[gp_data.category == "finance"]
        cash_install_unpopular_second_app = loan_app_data[
            (loan_app_data.install_total >= 10000)
            & (loan_app_data.install_total < 100000)]["package_name"].tolist()
        # 贷款安装低于10w-100w的app安装包名字
        loan_app_data = gp_data[gp_data.category == "finance"]
        cash_install_middle_app = loan_app_data[
            (loan_app_data.install_total >= 100000)
            & (loan_app_data.install_total < 1000000)]["package_name"].tolist()
        # 贷款安装10w的app安装包名字
        loan_app_data = gp_data[gp_data.category == "finance"]
        cash_install_popular_app = loan_app_data[(
                loan_app_data.install_total >= 1000000)]["package_name"].tolist()
        popular_app_dict["review_unpopular_app"] = review_unpopular_app
        popular_app_dict[
            "review_unpopular_second_app"] = review_unpopular_second_app
        popular_app_dict["review_middle_app"] = review_middle_app
        popular_app_dict["review_popular_app"] = review_popular_app
        popular_app_dict["install_unpopular_app"] = install_unpopular_app
        popular_app_dict[
            "install_unpopular_second_app"] = install_unpopular_second_app
        popular_app_dict["install_middle_app"] = install_middle_app
        popular_app_dict["install_popular_app"] = install_popular_app
        popular_app_dict["cash_install_unpopular_app"] = cash_install_unpopular_app
        popular_app_dict[
            "cash_install_unpopular_second_app"] = cash_install_unpopular_second_app
        popular_app_dict["cash_install_middle_app"] = cash_install_middle_app
        popular_app_dict["cash_install_popular_app"] = cash_install_popular_app
        gp_package_name = gp_data["package_name"].tolist()
        calss_category_dict = {
            package: category
            for package, category in zip(gp_package_name, gp_class_category)
        }
        gp_cash_loan_app_package = gp_data[gp_data.category ==
                                           "finance"]["package_name"].tolist()
        #joblib.dump(calss_category_dict,
                    #"./app_data_files/calss_category_dict.pkl")
        #joblib.dump(gp_cash_loan_app_package,
                    #"./app_data_files/gp_cash_loan_app_package.pkl")
        #joblib.dump(popular_app_dict, "./app_data_files/popular_app_dict.pkl")
        # return popular_app_dict
        #print("文件写入完成")

    def load_app_dicts(self):
        """载入各种映射的app字典"""
        calss_category_dict = joblib.load("calss_category_dict.pkl")
        gp_cash_loan_app_package = joblib.load("gp_cash_loan_app_package.pkl")
        popular_app_dict = joblib.load("popular_app_dict.pkl")
        return calss_category_dict, gp_cash_loan_app_package, popular_app_dict

    def get_unique_values(self, lis, rever=False):
        dict_unique = dict(zip(*np.unique(lis, return_counts=True)))
        if rever:
            return dict(
                sorted(dict_unique.items(), key=lambda d: d[1], reverse=True))
        else:
            return dict_unique

    def popular_app_cnt(self, parse_data=None, apps_list=[]):
        """data 解析后的客户appDataFrame, apps_list 冷热门 package name"""
        cnt = 0
        package_names = []
        for name in parse_data.packageName.tolist():
            if name in apps_list:
                cnt += 1
                package_names.append(name)
        return cnt

    def static_popular_app(self, parse_data=None, popular_app_dict=None, types="install"):
        """统计不同热度app安装的个数"""
        popular_app_cnt_dict = {}
        for popular in popular_app_dict.keys():
            popular_app_cnt_dict[f"{popular}_{types}"] = self.popular_app_cnt(
                parse_data=parse_data, apps_list=popular_app_dict[popular])
        return popular_app_cnt_dict

    def classify_cash_app(self, parse_data=None, gp_cash_loan_app_package=None, types="install"):
        """区分gp上安装的贷款app 与非gp 安装贷款app
            gp_cash_loan_app_package : 离线将gp 贷款app保存好
            cash_app_package 客户手机里匹配到的 贷款app
        """
        cash_app_dict = {}
        cash_words = [
            'dana', 'cash', 'kredit', 'pinjaman', 'pinjam', 'dompet', 'ksp', 'ojk',
            'darurat', 'tunai', 'modal', 'finance', 'cepat', 'keuangan', 'uang',
            'loan', 'saku', 'tagihan', 'lender', 'money', 'uangku', 'kami', 'darurat',
            'emas', 'loan', 'kantong', 'duit', 'rupiah', 'uangme', 'ekpressuang',
            'pinjaman', 'kredi', 'online', 'uang', 'dan'
        ]
        app_name = parse_data.appName.tolist()
        cash_app = []
        for cash in cash_words:
            for i in app_name:
                if i.find(cash) >= 0:
                    cash_app.append(i)
        cash_app_package = parse_data[parse_data.appName.isin(cash_app)].packageName.tolist()
        # 非gp 安装的贷款app
        nonunion_cash_app = [
            i for i in cash_app_package if i not in gp_cash_loan_app_package
        ]
        # gp安装的贷款app
        union_cash_app = [i for i in cash_app_package if i in gp_cash_loan_app_package]
        cash_app_dict[f"cash_app_cnt_{types}"] = len(cash_app_package)
        cash_app_dict[f"gp_cash_app_cnt_{types}"] = len(union_cash_app)
        cash_app_dict[f"un_gp_cash_app_cnt_{types}"] = len(nonunion_cash_app)
        return cash_app_dict

    def change_key(self, parse_data=None, types="install"):
        static_category_new = {}
        class_category_keys = [
            'social', 'gamble', 'entertainment', 'lifestyle', 'video_music', 'communication', 'personal', 'game',
            'company', 'ebook',
            'business', 'travel', 'tool', 'education', 'finance', 'others'
        ]
        static_category = self.get_unique_values(parse_data.category)
        for key in class_category_keys:
            if key not in static_category.keys():
                static_category[key] = 0
        for key in static_category.keys():
            static_category_new[f"{key}_{types}"] = static_category.get(key, 0)
        # static_category_new[f"{types}_cnt"]=len(parse_data)
        return static_category_new

    def static_app_species(self, parse_data=None, types="update"):
        static_species = {}
        class_category_keys = [
            'social', 'gamble', 'entertainment', 'lifestyle', 'video_music', 'communication', 'personal', 'game',
            'company', 'ebook',
            'business', 'travel', 'tool', 'education', 'finance', 'others'
        ]
        # static_category = get_unique_values(parse_data.category)
        calss_category_dict, gp_cash_loan_app_package, popular_app_dict = self.load_app_dicts(
        )
        parse_data["category"] = parse_data.packageName.map(calss_category_dict)
        parse_data["category"] = parse_data["category"].fillna("others")
        static_species[f"app_cnt_{types}"] = len(parse_data)
        static_popular_update = self.static_popular_app(
            parse_data=parse_data[parse_data.is_update > 0],
            popular_app_dict=popular_app_dict,
            types=types)
        classify_cash_update = self.classify_cash_app(
            parse_data=parse_data[parse_data.is_update > 0],
            gp_cash_loan_app_package=gp_cash_loan_app_package,
            types=types)
        #     for key in class_category_keys:
        #         if key not in static_category.keys():
        #             static_category[key] = 0
        # static_category_update = get_unique_values(parse_data_data[parse_data_data.is_update>0].category)
        static_category_update = self.change_key(parse_data=parse_data, types=types)
        if static_species[f"app_cnt_{types}"] > 0:
            for app_species in class_category_keys:
                static_species[
                    f"{app_species}_to_{types}_ratio"] = static_category_update.get(
                    f"{app_species}_{types}",
                    0) / (static_species.get(f"app_cnt_{types}", 0) + 10e-9)
            for cash_species in classify_cash_update.keys():
                # print(cash_species)
                static_species[
                    f"{cash_species}_to_{types}_ratio"] = classify_cash_update.get(
                    f"{cash_species}",
                    0) / (static_species.get(f"app_cnt_{types}", 0) + 10e-9)
            for popular_species in static_popular_update.keys():
                # print(popular_species)
                static_species[
                    f"{popular_species}_to_{types}_ratio"] = static_popular_update.get(
                    f"{popular_species}",
                    0) / (static_species.get(f"app_cnt_{types}", 0) + 10e-9)
        else:
            for app_species in class_category_keys:
                static_species[f"{app_species}_to_{types}_ratio"] = np.NaN
            for cash_species in classify_cash_update.keys():
                # print(cash_species)
                static_species[f"{cash_species}_to_{types}_ratio"] = np.NaN
            for popular_species in static_popular_update.keys():
                # print(popular_species)
                static_species[f"{popular_species}_to_{types}_ratio"] = np.NaN

        return {
            **classify_cash_update,
            **static_popular_update,
            **static_species,
            **static_category_update
        }

    def app_change(self, parse_data=None):
        """更新与未更新变化"""
        keys_list = [
            'cash_app_cnt', 'gp_cash_app_cnt', 'un_gp_cash_app_cnt',
            'review_unpopular_app', 'review_unpopular_second_app',
            'review_middle_app', 'review_popular_app', 'install_unpopular_app',
            'install_unpopular_second_app', 'install_middle_app',
            'install_popular_app', 'cash_install_unpopular_app',
            'cash_install_unpopular_second_app', 'cash_install_middle_app',
            'cash_install_popular_app', 'app_cnt', 'travel', 'company', 'others', 'business',
            'entertainment', 'tool', 'game', 'lifestyle', 'ebook', 'social', 'video_music', 'finance', 'communication',
            'gamble', 'personal',
            'education'
        ]
        notupdate_cnt_keys = [
            'cash_app_cnt_notupdate', 'gp_cash_app_cnt_notupdate',
            'un_gp_cash_app_cnt_notupdate', 'review_unpopular_app_notupdate',
            'review_unpopular_second_app_notupdate', 'review_middle_app_notupdate',
            'review_popular_app_notupdate', 'install_unpopular_app_notupdate',
            'install_unpopular_second_app_notupdate',
            'install_middle_app_notupdate', 'install_popular_app_notupdate',
            'cash_install_unpopular_app_notupdate',
            'cash_install_unpopular_second_app_notupdate',
            'cash_install_middle_app_notupdate',
            'cash_install_popular_app_notupdate', 'app_cnt_notupdate',
            'company_notupdate', 'others_notupdate', 'tool_notupdate', 'ebook_notupdate',
            'finance_notupdate', 'social_notupdate', 'gamble_notupdate', 'entertainment_notupdate',
            'lifestyle_notupdate', 'video_music_notupdate', 'communication_notupdate', 'personal_notupdate',
            'game_notupdate', 'business_notupdate', 'travel_notupdate', 'education_notupdate'
        ]
        install_cnt_keys = [
            'cash_app_cnt_install', 'gp_cash_app_cnt_install',
            'un_gp_cash_app_cnt_install', 'review_unpopular_app_install',
            'review_unpopular_second_app_install', 'review_middle_app_install',
            'review_popular_app_install', 'install_unpopular_app_install',
            'install_unpopular_second_app_install', 'install_middle_app_install',
            'install_popular_app_install', 'cash_install_unpopular_app_install',
            'cash_install_unpopular_second_app_install',
            'cash_install_middle_app_install', 'cash_install_popular_app_install',
            'app_cnt_install', 'travel_install', 'company_install', 'others_install',
            'business_install', 'entertainment_install', 'tool_install', 'game_install',
            'lifestyle_install', 'ebook_install', 'social_install', 'video_music_install',
            'finance_install', 'communication_install', 'gamble_install', 'personal_install', 'education_install'
        ]
        update_cnt_kyes = [
            'cash_app_cnt_update', 'gp_cash_app_cnt_update',
            'un_gp_cash_app_cnt_update', 'review_unpopular_app_update',
            'review_unpopular_second_app_update', 'review_middle_app_update',
            'review_popular_app_update', 'install_unpopular_app_update',
            'install_unpopular_second_app_update', 'install_middle_app_update',
            'install_popular_app_update', 'cash_install_unpopular_app_update',
            'cash_install_unpopular_second_app_update',
            'cash_install_middle_app_update', 'cash_install_popular_app_update',
            'app_cnt_update', 'travel_update', 'company_update', 'others_update',
            'business_update', 'entertainment_update', 'tool_update', 'game_update', 'lifestyle_update',
            'ebook_update', 'social_update', 'video_music_update', 'finance_update', 'communication_update',
            'gamble_update', 'personal_update', 'education_update'
        ]
        install_static = self.static_app_species(parse_data=parse_data, types="install")
        update_static = self.static_app_species(
            parse_data=parse_data[parse_data.is_update > 0], types="update")
        notupdate_static = self.static_app_species(
            parse_data=parse_data[parse_data.is_update < 1], types="notupdate")
        up_to_not_up_dict = {}
        install_to_up_dict = {}
        for update, notupdate, key in zip(update_cnt_kyes, notupdate_cnt_keys,
                                          keys_list):
            up_to_not_up_dict[f"{key}_up_to_not_minus"] = update_static[
                                                              update] - notupdate_static[notupdate]
            up_to_not_up_dict[f"{key}_up_to_not_increase"] = (
                                                                     update_static[update] -
                                                                     notupdate_static[notupdate]) / (
                                                                     update_static[update] + 10e-9)
            if notupdate_static[notupdate] > 0:
                up_to_not_up_dict[f"{key}_up_to_not_ratio"] = update_static[
                                                                  update] / (notupdate_static[notupdate] + 10e-9)
            else:
                up_to_not_up_dict[f"{key}_up_to_not_ratio"] = np.NaN
        for update, install, key in zip(update_cnt_kyes, install_cnt_keys,
                                        keys_list):
            install_to_up_dict[f"{key}_up_to_install_ratio"] = update_static[
                                                                   update] / (install_static[install] + 10e-9)
        install_to_up_dict["app_cnt_update_ratio"] = update_static[
                                                         "app_cnt_update"] / (install_static["app_cnt_install"] + 10e-9)
        install_to_up_dict["app_cnt_notupdate_ratio"] = 1 - install_to_up_dict[
            "app_cnt_update_ratio"]
        return {
            **install_static,
            **update_static,
            **notupdate_static,
            **up_to_not_up_dict,
            **install_to_up_dict
        }

    def no_time_slice_feat(self, parse_data=None):
        cnt = 0
        feat = {}
        famous_app = [
            'drive', 'ovo', 'akulaku', 'loan segara', 'whatsapp', 'digi dana',
            'kredivo', 'gojek', 'messenger', 'pinjamango', 'grant rupiah',
            'kta kilat', 'kredit pintar', 'easycash', 'my home credit indonesia',
            'tunaicepat', 'shopee', 'facebook', 'adakami', 'durian runtuh',
            'rupiah cepat', 'pinjam yuk', 'uangme', 'uku', 'uc browser', 'lazada',
            'duitkita', 'instagram', 'danarupiah', 'twitter', 'tiktok', 'grab',
            'mytelkomsel'
        ]
        for app in famous_app:
            if app in parse_data.appName.tolist():
                feat[f"{app}_install_to_now_days"] = parse_data[
                    parse_data.appName == app]["install_to_create_days"].iloc[0]
                feat[f"{app}_update_to_now_days"] = parse_data[
                    parse_data.appName == app]["update_to_create_days"].iloc[0]
                feat[f"{app}_ins_upd_diff_days"] = feat[
                                                       f"{app}_install_to_now_days"] - feat[
                                                       f"{app}_update_to_now_days"]
                feat[f"has_{app}"] = 1
                cnt += 1
            else:
                feat[f"{app}_install_to_now_days"] = np.NaN
                feat[f"{app}_update_to_now_days"] = np.NaN
                feat[f"{app}_ins_upd_diff_days"] = np.NaN
                feat[f"has_{app}"] = 0
        feat["famous_app_cnt"] = cnt
        feat["famous_app_cnt_ratio"] = cnt / len(famous_app)
        feat["mean_update_days"] = np.mean(parse_data.install_to_create_days -
                                           parse_data.update_to_create_days)
        feat["var_update_days"] = np.var(parse_data.install_to_create_days -
                                         parse_data.update_to_create_days)
        # feat["medain_update_days"]=np.median(parse_data.install_to_create_days-parse_data.update_to_create_days)
        feat["std_update_days"] = np.std(parse_data.install_to_create_days -
                                         parse_data.update_to_create_days)
        feat["max_update_days"] = max(parse_data.install_to_create_days -
                                      parse_data.update_to_create_days)
        feat["min_update_days"] = min(parse_data.install_to_create_days -
                                      parse_data.update_to_create_days)
        feat["std_update_days"] = np.std(parse_data.install_to_create_days -
                                         parse_data.update_to_create_days)
        feat["max_install_date"] = int(max(parse_data.install_to_create_days))
        feat["min_install_date"] = int(min(parse_data.install_to_create_days))
        feat["system_app_cnt"] = parse_data.system_app.sum()
        return feat

    def slice_static_app(self, parse_data=None, last_days="totle", show_dataframe=True):
        static_app_dict_ = {}
        static_app_dict = self.app_change(parse_data=parse_data)
        for key in static_app_dict.keys():
            static_app_dict_[f"last_{last_days}_days_{key}"] = static_app_dict[key]
        if show_dataframe:
            return pd.DataFrame.from_dict(static_app_dict_, orient="index").T
        return static_app_dict_
    
    def add_feats(self, parse_data=None, dataframe=True):
        days = [7, 30, 60, 90, 100000]
        day_srs = [3, 7, 14, 30, 60, 90, "totle"]
        # days = [3, 100]
        # day_srs = [3,7, 100]
        feat_dict = {}
        for day, day_sr in zip(days, day_srs):
            feat_dict.update(
                self.slice_static_app(
                    parse_data=parse_data[parse_data.install_to_create_days <= day],
                    last_days=day_sr, show_dataframe=False))
        all_feats = {**feat_dict, **self.no_time_slice_feat(parse_data=parse_data)}
        cols = [
            'tar', 'item_code', 'allplatfrom_user_type', 'is_first_overdue', 'sex',
            'education_level', 'company_province', 'famil_province',
            'company_month_income', 'begin_work_year', 'same_place', 'request_date',
            'liveInDate', 'age', 'company_job', 'sell_accounts_date', 'device_model',
            'decive_brand', 'to_now_live_days', 'is_switch_pages',
            'read_privacy_agreement_time', 'read_privacy_agreement_times',
            'internet_type', 'boot_time', 'battery_power', 'screen_rate_long',
            'screen_rate_width'
        ]
        all_feats["app_user_id"] = parse_data["app_user_id"].iloc[0]
        all_feats["repay_date"] = parse_data["repay_date"].iloc[0]
        all_feats["bayes_prob"] = parse_data["bayes_prob"].iloc[0]#新增的贝叶斯概率维度
        for col in cols:
            all_feats[col] = parse_data[col].iloc[0]
        if dataframe:
            return pd.DataFrame.from_dict(all_feats, orient="index").T
        return all_feats


if __name__ == '__main__':
    time1 = time.time()
    parse_data = Request_data().parse_applist(db='dopemt',user_id="bbbc94a7d8a24738a72539a37e2e4229")
    time2 = time.time()
    print(f"解析数据耗时 {round((time2 - time1), 3)} s")
    feats = AddFeatures().add_feats(parse_data=parse_data, dataframe=True)
    print(feats.allplatfrom_user_type)
    time3 = time.time()
    cost_time = time3 - time2
    print(f"衍生特征变量纬度：{feats.shape[1]},耗时{round(cost_time, 3)} s")

